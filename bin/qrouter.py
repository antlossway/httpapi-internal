#!/usr/bin/env python3
"""
check subdir under ~/sendxms/SERVER_SUPER100/received/
for each subdir process batch files
for each file
1. check routing and move file to output SMPP folder
2. insert into cdr (insert to redis, insert_cdr_from_redis_cache.py will take care insertion to postgresql DB)
"""

import time
import os
import signal
import sys
from pathlib import Path
import logging
import re
import random
import site
from collections import defaultdict
import json

basedir = os.path.abspath(os.path.dirname(__file__)) #/home/amx/bin/
libdir = os.path.join(basedir, "../pylib")
site.addsitedir(libdir)
import DB

ext = '' # http
try:
    ext = sys.argv[1]
except:
    pass

instance = os.path.basename(__file__).split(".")[0]
if ext != '':
    instance += '-' + ext

log = os.path.join(basedir, f"../log/{instance}.log") #/home/amx/log/qrouter.log
lockfile= os.path.join(basedir, f"../var/lock/{instance}.lock")
trash_dir = os.path.join(basedir, "../trash")

##########################
#### configure logger ####
##########################
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

##########################
#### global variables ####
##########################
db = DB.connectdb()
if not db:
    logger.warning("!!! cannot connect to PostgreSQL DB")
    exit()
cur = db.cursor()

r = DB.connect_redis()
if not r:
    logger.warning("!!! cannot connect to redis server")
    exit()

batch = 2
d_ac = dict() # id => name
d_smpp_ac = dict() #keep smpp account info, id => name---billing_id---product_id---directory
d_http_ac = dict() #keep http account info, id => name---billing_id---product_id---api_key
d_customer_operator_routing = dict()
d_provider = dict() #id => name
d_smsc = dict() # id => name
d_smsc_dir = dict() # id => directory
d_smsc_provider = dict() # smsc_id => provider_id
d_provider_smsc = defaultdict(list) #provider_id => [smsc_id1, smsc_id2]
d_country = dict()
d_operator = dict()
np = DB.get_numbering_plan(cur) #numbering_plan
logger.info(f"numbering_plan entry {len(np)}")

#### regex to match key info in SMS file
r_bnumber = re.compile("^Phone=(.*)")
r_xms = re.compile("^XMS=(.*)")
r_tpoa = re.compile("^OriginatingAddress=(.*)")
r_udh = re.compile("^UDH=(.*)")
r_msgid = re.compile("^MsgId=(.*)")
r_dcs = re.compile("^DCS=(.*)")
r_split = re.compile("^Split=(.*)")
r_server = re.compile("SERVER")
r_action = re.compile("^Action=(.*)")

#### regex to clean bnumber
r_leading_plus = re.compile(r'^\++')
r_leading_zero = re.compile(r'^0+')

r_msisdn = re.compile("^\+?\d+$")

#### regex to match OPTOUT MO, get out keyword if there is any, keyword is to identify brand
#r_optout_with_keyword = re.compile(r'(optout|stop)\s+(\w+)',re.IGNORECASE)
#r_optout = re.compile(r'(optout|stop)',re.IGNORECASE)

def print_dict(data,name):
    for k,v in data.items():
        logger.info(f"{name}: {k} => {v}")

def read_config():

    logger.info("======= read_config =======")
    sql = f"select id,name,billing_id,product_id,connection_type,directory,api_key from account where live=1;"
    cur.execute(sql)
    rows = cur.fetchall()
    if rows:
        d_ac.clear() #reset d_ac
        d_smpp_ac.clear()
        d_http_ac.clear()

        for row in rows:
            (acid,acname,billing_id,product_id,conn_type,in_dir,api_key) = row
            d_ac[acid] = acname
            if conn_type == 'smpp':
                d_smpp_ac[acid] = f"{acname}---{billing_id}---{product_id}---{in_dir}"
            else:
                #d_http_ac[acid] = f"{acname}---{billing_id}---{product_id}---{api_key}"
                d_http_ac[api_key] = f"{acname}---{billing_id}---{product_id}---{acid}"
    else:
        logger.warning("!!! query table account return empty result, keep existing data")

    print_dict(d_smpp_ac,'smpp account')

    sql = f"select product_id,country_id,operator_id,provider_id from customer_operator_routing;"
    cur.execute(sql)
    rows = cur.fetchall()
    if rows:
        d_customer_operator_routing.clear()
        for row in rows:
            (product_id,country_id,operator_id,provider_id) = row
            d_customer_operator_routing[f"{product_id}---{country_id}---{operator_id}"] = provider_id
    else:
        logger.warning("!!! query table customer_operator_routing return empty result, keep existing data")

    print_dict(d_customer_operator_routing,'customer_operator_routing')
    

    sql = f"select id,name from provider;"
    cur.execute(sql)
    rows = cur.fetchall()
    if rows:
        d_provider.clear()
        for row in rows:
            (provider_id,provider_name) = row
            d_provider[provider_id] = provider_name
    else:
        logger.warning("!!! query table provider return empty result, keep existing data")
    print_dict(d_provider,'provider')

    sql = f"select id,name from countries;"
    cur.execute(sql)
    rows = cur.fetchall()
    if rows:
        d_country.clear()
        for row in rows:
            (id,name) = row
            d_country[id] = name
    else:
        logger.warning("!!! query table countries return empty result, keep existing data")
    print_dict(d_country,'countries')

    sql = f"select id,name from operators;"
    cur.execute(sql)
    rows = cur.fetchall()
    if rows:
        d_operator.clear()
        for row in rows:
            (id,name) = row
            d_operator[id] = name
    else:
        logger.warning("!!! query table operators return empty result, keep existing data")
    print_dict(d_operator,'operators')

    sql = f"select id,name,provider_id,directory from smsc"
    cur.execute(sql)
    rows = cur.fetchall()
    if rows:
        d_smsc.clear()
        d_provider_smsc.clear()

        for row in rows:
            (smsc_id,smsc_name,provider_id,directory) = row
            d_smsc[smsc_id] = smsc_name
            d_smsc_dir[smsc_id] = directory
            d_provider_smsc[provider_id].append(smsc_id)
            d_smsc_provider[smsc_id] = provider_id
    else:
        logger.warning("!!! query table smsc return empty result, keep existing data")
    print_dict(d_smsc,'smsc')
    print_dict(d_smsc_dir,'smsc_dir')
    print_dict(d_provider_smsc,'provider_smsc mapping')
    print_dict(d_smsc_provider,'smsc_provider mapping')


    logger.info("===============================")

def clean_bnumber(bnumber):
    bnumber = re.sub(r_leading_plus, r'',bnumber)
    bnumber = re.sub(r_leading_zero, r'',bnumber)
    bnumber = re.sub(r'^',r'+',bnumber)
    return bnumber

def leave(signal, frame): #INT, TERM
    logger.info(f"received signal {signal}, will exit")
    os.unlink(lockfile)
    cur.close()
    db.close()
    sys.exit()

def reload_config(signal, frame): #USR1
    logger.info(f"### received signal {signal}, reload_config")
    read_config()
    return

def save_sql(sql):
    logger.info(sql)
    if r.lpush('cdr_cache',sql): #successful transaction return True
        logger.info(f"LPUSH cdr_cache OK")
    else:
        logger.warning(f"!!! problem to LPUSH cdr_cache {sql}")

def scandir(acid):
    acname,billing_id,product_id,in_dir = d_smpp_ac.get(acid).split("---")
    e = Path(in_dir)

    count = 0
    for myfile in e.iterdir():
        if count > batch:
            logger.info(f"processed {count} sms for {acname}({acid}) {in_dir}")
            break
        #if myfile.is_file() and re.match("^xms",os.path.basename(myfile)):
        if os.path.getsize(myfile) > 0 and re.match("^xms",os.path.basename(myfile)):
            if process_file(myfile,acid):
                count += 1 

def scan_redis_queue(api_key):
    acname,billing_id,product_id,acid = d_http_ac.get(api_key).split("---")
    ## rpop from list HTTPIN:{api_key}
    queue_in = f"HTTPIN:{api_key}"
    count = 0
    while(r.llen(queue_in) > 0):
        if count > batch:
            logger.info(f"processed {count} sms for {acname}({acid}) {api_key}")
            break
        msgid = r.rpop(queue_in).decode("utf-8")
        logger.info(f"### LPOP from {queue_in}: {msgid}")
        ### find sms detail from HASH HTTPSMS:{msgid}
        index = f"HTTPSMS:{msgid}"
        res = r.hgetall(index)
        d_sms = { k.decode('utf-8'): res.get(k).decode('utf-8') for k in res.keys() }
        logger.info(f"### HGETALL {index}")
        logger.info(json.dumps(d_sms,indent=4))

        process_sms(d_sms,api_key,None)
        count += 1


def get_route(product_id,cid,opid):
    ###smsc_id---provider_id---output_dir
    #return "1---6---/home/xqy/dev/python3/fastapi/httpapi/sendxms/CMI_PREMIUM1/spool/CMI_PREMIUM1"

    ### product_id---country_id---operator_id => provider_id
    pattern_default = f"4---4---4"
    pattern_all = f"{product_id}---4---4"
    pattern_c_all = f"{product_id}---{cid}---4"
    pattern_op = f"{product_id}---{cid}---{opid}"

    if pattern_op in d_customer_operator_routing:
        provider_id = d_customer_operator_routing.get(pattern_op)
    elif pattern_c_all in d_customer_operator_routing:
        provider_id = d_customer_operator_routing.get(pattern_c_all)
    elif pattern_all in d_customer_operator_routing:
        provider_id = d_customer_operator_routing.get(pattern_all)
    else:
        provider_id = d_customer_operator_routing.get(pattern_default)

    logger.info(f"get_route find provider {d_provider.get(provider_id)} ({provider_id}) for {d_country.get(cid)}/{d_operator.get(opid)}")

    ### distribute to smsc
    smsc_id = random.choice(d_provider_smsc.get(provider_id))
    logger.info(f"get_route pick smsc {d_smsc.get(smsc_id)} ({smsc_id})")

    return smsc_id

def generate_dlr():
    ### TBD
    logger.info("TBD: generate reject DLR")
    pass

def process_file(myfile,acid):
    d_sms = dict()
    logger.info(f"process_file: {myfile} for acid {acid}")
    logger.info("=========================")
    with open(myfile,'r') as reader:
        for line in reader: #same as: for line in reader.readlines():
            line = line.strip()
            logger.info(line)
            #if(z := r_xms.match(line)):
                #xms = z.groups()[0]
            if r_xms.match(line):
                d_sms['xms'] = r_xms.match(line).groups()[0]
            elif r_bnumber.match(line):
                d_sms['bnumber'] = r_bnumber.match(line).groups()[0]
            elif r_tpoa.match(line):
                tpoa = r_tpoa.match(line).groups()[0]
                tpoa = re.sub(r'^\d:\d:',r'',tpoa).strip() # 5:0:Routee => Routee
                d_sms['tpoa'] = tpoa
            elif r_udh.match(line):
                d_sms['udh'] = r_udh.match(line).groups()[0]
            elif r_dcs.match(line):
                d_sms['dcs'] = r_dcs.match(line).groups()[0]
            elif r_msgid.match(line):
                d_sms['msgid'] = r_msgid.match(line).groups()[0]
            elif r_split.match(line):
                d_sms['split'] = r_split.match(line).groups()[0]
            elif r_action.match(line):
                d_sms['action'] = r_action.match(line).groups()[0]

    logger.info("=========================")

    logger.info("### debug d_sms")
    logger.info(json.dumps(d_sms,indent=4))

    process_sms(d_sms,acid,myfile)

    return 1


def process_sms(d_sms,acid,myfile): #for HTTP incoming, acid is api_key
    if myfile:
        acname,billing_id,product_id,in_dir = d_smpp_ac.get(acid).split("---")
    else:
        api_key = acid
        acname,billing_id,product_id,acid = d_http_ac.get(api_key).split("---")

    bnumber,msgid,xms,tpoa,udh,action = '','','','','',''
    dcs,error,split = 0,0,1
    route = ''
    to_trash= 0
    tpoa_status = 2000 
    insert_optout = 0 #if it's optout MO, insert into table optout, so future Promotion should not be sent to this number

    tpoa = d_sms.get('tpoa')
    bnumber = d_sms.get('bnumber')
    xms = d_sms.get('xms')
    udh = d_sms.get('udh',udh)
    dcs = d_sms.get('dcs',dcs)
    msgid = d_sms.get('msgid')
    split = d_sms.get('split',split)
    action = d_sms.get('action',action)

    bnumber = clean_bnumber(bnumber)
    #print(f"debug: numbering_plan has {len(np)} entries")
    result = DB.parse_bnumber(np, bnumber)
    if result == None:
        logger.info(f"!!! {bnumber} does not belong to any network")
        ### delete file
        if myfile: # redis HASH HTTPSMS:{msgid} will expire by itself
            os.remove(myfile)
        generate_dlr()
        return 0
    else:
        cid,opid = result.split('---')
        cid = int(cid)
        opid = int(opid)

    xms_len = len(xms)

    #### trash DLR , only process MO
    if action == 'Status': #only happen for smpp
        logger.info(f"!!! trash DLR")
        to_trash = 1
        outfile = os.path.join(trash_dir, os.path.basename(myfile))
    else:
        smsc_id = get_route(product_id,cid,opid)
        smsc_name = d_smsc.get(smsc_id)
        provider_id = d_smsc_provider.get(smsc_id)
        
        outdir = d_smsc_dir.get(smsc_id)
        tmpdir = outdir + '/tmp'
        if myfile:
            outfile = os.path.join(outdir, os.path.basename(myfile))
            tmpsms = os.path.join(tmpdir, os.path.basename(myfile))
        else:
            outfile = os.path.join(outdir, f"xms{msgid}")
            tmpsms = os.path.join(tmpdir, f"xms{msgid}")
        
    if to_trash == 1: #only happen for smpp
        os.rename(myfile,outfile)
        logger.info(f"trash {myfile} to {outfile}")

    else:
        if myfile:
            os.remove(myfile)
            logger.info(f"delete input file {myfile}")
    
        msg_submit = f"""\
; encoding=UTF-8
[{smsc_name.upper()}]
Phone={bnumber}
OriginatingAddress={tpoa}
Priority=0
XMS={xms}
DCS={dcs}
LocalId={msgid}
StatusReportRequest=1
"""
        if int(split) > 1:
            msg_submit += f"Split={split}\n"
        if udh != '':
            msg_submit += f"UDH={udh}\n"

        with open(tmpsms,'w') as w:
            w.write(msg_submit)
        logger.info("=========================")
        logger.info(msg_submit)
        logger.info("=========================")

        os.rename(tmpsms,outfile)
        logger.info(f"rename {tmpsms} to {outfile}")
       
        #treat single quote before inserting to postgresql
        tpoa = re.sub("'","''",tpoa)
        xms = re.sub("'","''",xms)
        xms = xms[:400]
  
        save_sql(f"insert into cdr (msgid,account_id,billing_id,product_id,tpoa,bnumber,country_id,operator_id,dcs,len,split,udh,provider_id,smsc_id,xms) values ('{msgid}',{acid},{billing_id},{product_id},'{tpoa}','{bnumber}',{cid},{opid},{dcs},{xms_len},{split},'{udh}',{provider_id},{smsc_id},'{xms}')")

    return 1
    
def check_pid_running(pid):
    '''Check For the existence of a unix pid.
    '''
    try:
        os.kill(pid,0)
    except OSError:
        return False
    else:
        return True
        
def main():
    pid = os.getpid()
    logger.info(f"Hey, {__file__} (pid: {pid}) is started!")

    try:
        with open(lockfile,'r') as f:
            oldpid = f.readline()
            oldpid.strip() #chomp
            if oldpid != '':
                while check_pid_running(int(oldpid)): #check_pid_running return true, means the process is running
                    logger.info(f"###### {__file__} {oldpid} is running, kill it and run my own")
                    os.kill(int(oldpid), signal.SIGTERM)
                    time.sleep(5)
    except FileNotFoundError:
        logger.info("no lock file, will create one")


    with open(lockfile,'w') as w:
        logger.info(f"create lockfile {lockfile}: {pid}")
        w.write(str(pid))
        
    signal.signal(signal.SIGINT, leave)
    signal.signal(signal.SIGTERM, leave)
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    signal.signal(signal.SIGUSR1, reload_config)

    read_config()

    ##### print out config #### 
    last_print = time.time()
    while True:
        show_alive = 0
        now = time.time()
        if now - last_print > 10: #every 10 sec, print something to show alive
            show_alive = 1
            last_print = now

        if ext == 'http':
            ### get sms from redis
            for api_key in d_http_ac.keys():
                if show_alive == 1:
                    acname,billing_id,product_id,acid = d_http_ac.get(api_key).split("---")
                    logger.info(f"scan_redis_queue for {acname}({acid}) HTTPIN:{api_key}")
                scan_redis_queue(api_key)

            time.sleep(1)
        else:
            for acid in d_smpp_ac.keys():
                if show_alive == 1:
                    acname,billing_id,product_id,in_dir = d_smpp_ac.get(acid).split("---")
                    logger.info(f"scandir for {acname}({acid}) {in_dir}")
                scandir(acid)
            
            time.sleep(1)

#       signal.pause()

if __name__ == '__main__':
    main()

