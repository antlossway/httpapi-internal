import os
from configparser import ConfigParser
import psycopg2 as pg
import re
import redis


def read_config(cfg):
    config = ConfigParser()
    config.read(cfg)
    
    return config

def connect_redis():
    basedir = os.path.abspath(os.path.dirname(__file__))
    cfg = os.path.join(basedir, "../etc/config.txt")

    config = read_config(cfg)

    redis_host = config['redis']['host']
    redis_port = config['redis']['port']

    r = redis.Redis(host=redis_host,port=redis_port)
    return r 

def connectdb():
    basedir = os.path.abspath(os.path.dirname(__file__))
    cfg = os.path.join(basedir, "../etc/config.txt")

    config = read_config(cfg)

    db_host = config['postgresql']['host']
    db_port = config['postgresql']['port']
    db_name = config['postgresql']['db']
    db_user = config['postgresql']['user']
    db_pass = config['postgresql']['password']

    db = None
    try:
        db = pg.connect("dbname={} user={} host={} password={} port={} ".format(db_name,db_user,db_host,db_pass,db_port))
        db.autocommit = True
  
    except Exception as error:
        print(error)
  
    return db

def get_numbering_plan(cur):
    np = {}
    sql = "select prefix,countryid,operatorid from numbering_plan;"
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            prefix = row[0]
            cid = row[1]
            opid = row[2]
            np[prefix] = f"{cid}---{opid}"
    except:
        print("!!! problem fetching numbering_plan")
        return None

    return np

def clean_msisdn(msisdn):
    number = msisdn.strip() #remove trailing whitespaces
    number = re.sub(r'^\++', r'',number) #remove leading +
    number = re.sub(r'^0+', r'',number) #remove leading 0

    if re.search(r'\D+',number): #include non-digit
        return None

    number = re.sub(r'^',r'+',number) #add back leading +

    if len(number) < 11 or len(number) > 16:
        return None

    return number

def parse_bnumber(np,msisdn):
    result = None
    while len(msisdn) > 0:
        #print(f"debug: parse_bnumber: {msisdn}")
        if msisdn in np.keys():
            result = np[msisdn]
            break
        else:
            msisdn = msisdn[:-1]  #remove last digit
    return result


if __name__ == '__main__':
   db = connectdb() 
   print("DB connected")
   cur = db.cursor()
   np = get_numbering_plan(cur)
   result = parse_bnumber(np, "+6586294138")
   print(result)
   db.close()
