#!/usr/bin/env python3

import requests
import json
import psycopg2 as pg
import time
import sys
import threading
from collections import defaultdict
import os
from concurrent.futures import ThreadPoolExecutor
import logging
import signal
from itertools import repeat
import re
from configparser import ConfigParser
import random
import smsutil
from uuid import uuid4

import site
basedir = os.path.abspath(os.path.dirname(__file__))
libdir = os.path.join(basedir, "../pylib")
site.addsitedir(libdir)
import DB

def read_config(cfg):
    config = ConfigParser()
    config.read(cfg)
    
    return config

#####################
## global variable ##
#####################
log_dir = os.path.join(basedir, "../log/")
lock_dir = os.path.join(basedir, "../var/lock/")

log = log_dir + "send_campaign.log"
lockfile= lock_dir + 'send_campaign.lock'

num_thread = 5 

cfg = os.path.join(basedir, "../etc/config.txt")
config = read_config(cfg)
master_key = config['api_test']['api_key']
master_secret = config['api_test']['api_secret']

notif1_expire = 4*24*3600
sms_expire = 3*24*3600 #redis HTTPSMS:{msgid}

#####################
## log configuration 
#####################

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.Formatter.converter = time.gmtime
# create a file handler
handler = logging.FileHandler(log)
handler.setLevel(logging.INFO)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
# add the handler to the logger
logger.addHandler(handler)

try:
    db = DB.connectdb()
    cur = db.cursor()
    logger.info("postgreSQL DB connected")
except Exception as error:
    logger.warning(f"!!! DB connection failed: {error}")
    exit()
try:
    r = DB.connect_redis()
    logger.info("redis connected")
except Exception as error:
    logger.warning(f"!!! redis connection failed: {error}")
    exit()

numbering_plan = DB.get_numbering_plan(cur)
logger.info(f"### get_numbering_plan: {len(numbering_plan)} entries")
logger.info(f"master_key: {master_key}, master_secret: {master_secret}")

def check_pid_running(pid):
    try:
        os.kill(pid,0)
    except OSError:
        return False
    else:
        return True

def leave(signal, frame): #INT, TERM
    logger.info(f"!!! receive signal {signal}, will exit")
    os.unlink(lockfile)

    sys.exit()
 
def get_cpg_list(cur,cpg_id):
    sql = f"""select hash,field_name,value from cpg_blast_list where cpg_id={cpg_id}"""
    logger.info(sql)
    d = defaultdict(dict)
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        (md5,field,value) = row
        d[md5][field] = value

    return d

#### same as in API mysms.py
def create_sms(ac,data): #ac: dict inclues account info, data: dict includes sms info
#        ac = {
#        "api_key": api_key,
#        "account_id": account_id,
#        "billing_id": billing_id,
#        "product_id": product_id,
#        }

#            data = {
#            "msgid": msgid,
#            "sender": sender,
#            "to": msisdn,
#            "content": xms,
#            "udh": udh
#            "dcs": dcs 
#            "require_dlr": 0 # campaign
#            "cpg_id": cpg_id # campaign
#            }

    logger.info(f"### create_sms debug: account")
    logger.info(json.dumps(ac, indent=4))
    logger.info(f"### create_sms debug: sms")
    logger.info(json.dumps(data, indent=4))

    api_key = ac.get('api_key')

    error = 0

    with r.pipeline() as pipe:
        ### lpush redis list HTTPIN:{api_key}: {msgid}
        redis_list = f"HTTPIN:{api_key}"
        r.lpush(redis_list, msgid)
        logger.info(f"##add msgid in list redis: LPUSH {redis_list} {msgid}")
        
        ### sms: hset redis HASH index HTTPSMS:{msgid}: 
        index = f"HTTPSMS:{msgid}"
        for k,v in data.items():
            r.hset(index,k,v)
            logger.info(f"## add SMS detail in redis: HSET {index} {k} {v}")
        r.expire(name=index, time=sms_expire) #expire in 3 days
        logger.info(f"#### redis: EXPIRE {index} {sms_expire}")
 
        ### notif1: hset redis HASH
        index_notif1 = f"{bnumber}:::{msgid}"
        value = f"HTTP:::{api_key}"
        r.hset(index_notif1,"CUSTOMER",value)
        logger.info(f"## record notif1 in redis: HSET {index_notif1} CUSTOMER {value}")
        r.expire(name=index_notif1, time=notif1_expire)
        logger.info(f"#### redis: EXPIRE {index_notif1} {notif1_expire}")

        pipe.execute()
  
    return error

def gen_udh_base():
        rand1 = random.randint(0,15)
        rand2 = random.randint(0,15)

        udh_base = "0003" + format(rand1,'X') + format(rand2, 'X')
        return udh_base
def gen_udh(udh_base,split,i):
    return udh_base + format(split,'02d') + format(i,'02d')
       
def send_sms(d, d_ac): #for each entry of blast list, create a SMS
    # d: dict = {
    #       'number':'6512355566',
    #       'var1':'variable'
    #}

#    d_ac = {
#       'billing_id': billing_id,
#       'account_id': account_id,
#       'product_id': product_id,
#       'api_key': api_key,
#       'sender':sender,
#       'xms': xms,
#       'cpg_id': cpg_id
#    }
# 
    logger.info(f"### send_sms debug: bnumber/variable")
    logger.info(json.dumps(d, indent=4))
    logger.info(f"### send_sms debug: account/sms")
    logger.info(json.dumps(d_ac, indent=4))

    #### get bnumber, already cleaned by API /internal/cpg
    bnumber = d.get('number',None)
    if not bnumber: #not supposed to happen
        logger.warning(f"!!! no bnumber found")
        return None

    ### parse_bnumber to get country_id, operator_id will be done by qrouter
    ### routing will be done by qrouter

    #del d['number']
    ### replace content template variable if there is any
    xms = d_ac.get('xms')
    api_key = d_ac.get('api_key')
    for field,value in d.items():
        pattern = f"%{field}%"
        try:
            xms = re.sub(pattern, value, xms)
        except Exception as err:
            logger.warning(f"!!! {err}")
    logger.info(f"final SMS content: {xms}")

    ### get split info
    sms = smsutil.split(xms)
    split = len(sms.parts)
    encoding = sms.encoding
    logger.info(f"counts of SMS: {split}")
    dcs = 0
    if not encoding.startswith('gsm'): #gsm0338 or utf_16_be
        dcs = 8 
    
    udh_base = ''
    udh = ''

    if split > 1:
        udh_base = gen_udh_base()
        logger.debug(f"gen_udh_base: {udh_base}")

    for i,part in enumerate(sms.parts):
        content = part.content
        msgid = str(uuid4())

        if udh_base != '':
            udh = gen_udh(udh_base,split,i+1)
            logger.debug(f"gen_udh: {udh}")

        data = {
            "msgid": msgid,
            "tpoa": d_ac.get("sender"),
            "bnumber": bnumber,
            "xms": content,
            "dcs": dcs,
            "cpg_id": d_ac.get("cpg_id")
        }


        if udh:
            data['udh'] = udh
    
        with r.pipeline() as pipe:
            ### lpush redis list HTTPIN:{api_key}: {msgid}
            redis_list = f"HTTPIN:{api_key}"
            r.lpush(redis_list, msgid)
            logger.info(f"##add msgid in list redis: LPUSH {redis_list} {msgid}")
            
            ### sms: hset redis HASH index HTTPSMS:{msgid}: 
            index = f"HTTPSMS:{msgid}"
            for k,v in data.items():
                r.hset(index,k,v)
                logger.info(f"## add SMS detail in redis: HSET {index} {k} {v}")
            r.expire(name=index, time=sms_expire) #expire in 3 days
            logger.info(f"#### redis: EXPIRE {index} {sms_expire}")
     
            ### notif1: hset redis HASH
            index_notif1 = f"{bnumber}:::{msgid}"
            value = f"HTTP:::{api_key}"
            r.hset(index_notif1,"CUSTOMER",value)
            r.hset(index_notif1,"require_dlr",0) #campaign does not need to return DLR
            logger.info(f"## record notif1 in redis: HSET {index_notif1} CUSTOMER {value}")
            logger.info(f"## record notif1 in redis: HSET {index_notif1} require_dlr 0")
            r.expire(name=index_notif1, time=notif1_expire)
            logger.info(f"#### redis: EXPIRE {index_notif1} {notif1_expire}")


def main():
    pid = os.getpid()
    logger.info(f"Hey, {__file__} (pid {pid} is started!")
    
    try:
        with open(lockfile, 'r') as f:
            oldpid = f.readline().strip()
            if oldpid != '':
                while check_pid_running(int(oldpid)):
                    logger.info("!!! program is running, kill it and run new one")
                    os.kill(int(oldpid), signal.SIGTERM)
                    time.sleep(5)
    except FileNotFoundError:
        logger.info(f"!!! no lock file {lockfile}, will create one")

    with open(lockfile, 'w') as w:
        logger.info(f"create lock file {lockfile}: {pid}")
        w.write(str(pid))

    signal.signal(signal.SIGINT, leave)
    signal.signal(signal.SIGTERM, leave)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
 
    while True:
        cpg_id = None
        ### select campaign
        cur.execute("""select cpg.id,cpg.tpoa,cpg.xms,cpg.billing_id,cpg.account_id,cpg.product_id, a.api_key from cpg 
                    join account a on cpg.account_id = a.id where status = 'TO_SEND' and sending_time < current_timestamp limit 1;""")
        try:
            (cpg_id,sender,xms,billing_id,account_id,product_id,api_key) = cur.fetchone()
        except:
            pass
        
        if cpg_id:
            d_ac = {
                'billing_id': billing_id,
                'account_id': account_id,
                'product_id': product_id,
                'api_key': api_key,
                'sender':sender,
                'xms': xms,
                'cpg_id': cpg_id
            }
       
            cur.execute(f"update cpg set status = 'SENDING' where id = {cpg_id}")
    
            #### get B-number list
            data = get_cpg_list(cur,cpg_id) #dict: md5 => {'number':'12355','var':'variable'}
            if len(data) > 0:
                start_time = time.time()
                for md5, d in data.items():
                    send_sms(d,d_ac)
         
                end_time = time.time()
                duration = int(end_time - start_time)
                logger.info(f"duration: {duration}")
            else:
                logger.warning(f"!!! no blast list found for cpg_id {cpg_id}")

            cur.execute(f"update cpg set status = 'SENT' where id = {cpg_id}")

        else:
            logger.info("Keep Alive")
        
        time.sleep(20)


if __name__ == '__main__':
    main()
