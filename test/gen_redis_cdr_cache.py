#!/usr/bin/python3
import redis
import os
from pathlib import Path
import re
import signal
import sys
import logging
import time
import random
import string
from uuid import uuid4

#####################
## global variable ##
#####################

basedir = os.path.abspath(os.path.dirname(__file__)) + "/../"
config_dir = basedir + "config/"
redis_cfg = config_dir + "redis.cfg"
list_name = "cdr_cache"

print(redis_cfg)

#redis_expire = 15*24*3600
#redis_expire = 30
#pipeline_batch = 2000

log = "/tmp/test.log"
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


###############
base_bnumber = '+628'
content_template = ['<ID> is your verfication code', 'PIN CODE: <ID>', '[TikTok] 028671 بمثابة رمز التحقق الخاص بك'] 
l_sender = ['Verify', 'Info', 'Notify']


def read_config():
    d = dict()
    with open(redis_cfg, 'r',encoding='utf-8') as f:
        lines = f.readlines()
        for line in lines:
            (k,v) = line.strip().split('=')
            if k == 'host':
                d[k] = v
            elif k == 'port' or k == 'db':
                d[k] = int(v)
    return d

   
def insert_with_pipeline(r,data):
    count = 0
    with r.pipeline() as pipe:
        for k,v in data.items():
            pipe.setex(k,redis_expire,value=v)
            logger.info(f"SETEX {k} {redis_expire} {v}")
            count += 1
        pipe.execute()
    
    logger.info(f"inserted {count} record")

def insert_normal(r,l):
    count = 0
    for v in l: 
        if r.lpush(list_name,v): #successful transaction return True
            logger.info(f"LPUSH {list_name} {v}")
            count += 1
        else:
            logger.warning(f"!!! problem to lpush {list_name} {v}")

    logger.info(f"inserted {count} record")

def gen_code(length=5): #default return random 5-digits
    digits = string.digits
    return ''.join( [ random.choice(digits) for n in range(length) ] )

def gen_bnumber(length=10): #default generate 10 digits
    bnumber = base_bnumber + gen_code(length)
    return bnumber

def gen_content():
    base = random.choice(content_template)
    code = gen_code() 
    xms = re.sub(r'<ID>', code, base)  # replace <ID> with random pin code
    return xms

def get_cid_opid(bnumber):
    return 95,95

def gen_cdr(num=10):
    l = list()
    for i in range(num):
        sender = random.choice(l_sender)
        bnumber = gen_bnumber()
        xms = gen_content()
        print(sender, bnumber, xms)

        udh = ''
        msgid = str(uuid4())
        country_id,operator_id = get_cid_opid(bnumber)
        
        sql = f"insert into cdr (webuser_id,billing_id,product_id,msgid,tpoa,bnumber,country_id,operator_id,dcs,len,udh,xms) values (1,1,0,'{msgid}','{sender}','{bnumber}',{country_id},{operator_id},0,{len(xms)},'{udh}','{xms}');"
        #sql = f"insert into cdr (webuser_id,billing_id,product_id,msgid,tpoa,bnumber,country_id,operator_id,dcs,len,udh,xms) values (1,1,0,'{msgid}','{sender}','{bnumber}',{country_id},{operator_id},0,{len(xms)},'{udh}');"

        print(f"### {sql}")
        l.append(sql)

    return l

def main():

    d_redis = read_config()
    for k,v in d_redis.items():
        logger.debug(f"redis param: {k} => {v} ({type(v)})")
    r = redis.Redis(host=f"{d_redis['host']}",port=f"{d_redis['port']}")

    try:
        r.ping()
        logger.info("redis server connected")
    except:
        logger.warning("!!! Can not connect redis server, leave")
        exit()

    l = gen_cdr(3)
    insert_normal(r,l)
   
if __name__ == '__main__':
    main()
