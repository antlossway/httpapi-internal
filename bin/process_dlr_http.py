#!/usr/bin/python3
"""
scan ~/sendxms/SERVER_HTTP/spool/

DLR filename is in format {api_key}---{provider}---{bnumber}---{notif3_msgid}---{msgid}
content:
 msisdn=6586294138
 msgid=61F6AD7E-B4D79-72D-7F9CD0A63700
 timestamp=2022-01-30 17:04:34
 to=NOC
 status=DELIVRD
 callback=http://example.com/callback

post DLR to callback url, different url may need to post different format

"""
import psycopg2
import redis
import os
import signal
import sys
import logging
import time
import re
from pathlib import Path
from configparser import ConfigParser

#####################
## global variable ##
#####################

basedir = os.path.abspath(os.path.dirname(__file__)) + "/../"

log_dir = basedir + "log/"
lock_dir = basedir + "var/lock/"

instance = os.path.basename(__file__)
instance = re.sub(r'\.py','',instance)

log = os.path.join(log_dir, f"{instance}.log")
lockfile= os.path.join(lock_dir, f"{instance}.lock")

spool = os.path.join(basedir, "sendxms/SERVER_HTTP/spool")
config_file = os.path.join(basedir, '.config')

print(f"log: {log}, lockfile: {lockfile}, config: {config_file}")
exit()

#####################
## Configuraiton  ###
#####################

def read_config():
    config = ConfigParser()

    logger.info(f"======= read_config {config_file}=======")
    ### reinitialize
    for section in config.sections():
        config.remove_section(section)

    config.read(config_file)

    for section in config.sections():
        logger.info(f"#### {section} ####")
        for key,value in config[section].items():
            logger.info(f"{key} => {value}")

    logger.info("===============================")
    return config

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

config = read_config()
db_host = config['postgresql']['host']
db_name = config['postgresql']['db']
db_user = config['postgresql']['user']
db_pass = config['postgresql']['password']

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
 
def scandir():
    e = Path(spool)

    for myfile in e.iterdir():
        logger.info(f"process {myfile}")

def main():
    pid = os.getpid()
    logger.info(f"Hey, {__file__} pid {pid} is started!")
    
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
 
    try:
        db = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_pass)
        cur = db.cursor()
        logger.info("postgreSQL DB connected")
    except Exception as error:
        logger.warning(f"!!! DB connection failed: {error}")
        exit()
    
    while True:
        count = 0
        while(r.llen(redis_cdr_list)!=0):
            to_commmit = 1
            count += 1
            sql = r.rpop(redis_cdr_list)
            logger.info(sql)
            ### TBD: pipeline insertion into postgres
            try:
                cur.execute(sql)
            except Exception as error:
                to_commit = 0
                logger.warning(f"!!! DB insert failure: {str(error).strip()}, move to redis error list {redis_error_list}")
                ### move to redis error list
                r.lpush(redis_error_list, sql)
            
            if to_commmit == 1:
                db.commit()

        if count == 0:
            logger.info("Keep Alive")
        
        time.sleep(10)
    
if __name__ == '__main__':
    main()
