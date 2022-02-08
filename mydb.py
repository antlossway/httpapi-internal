import psycopg2
import redis
import os
from collections import defaultdict
import json

from myutils import config,logger

########################
### Global Variables ###
########################
g_userinfo = defaultdict(dict) #info from legacy table 
g_account = defaultdict(dict) #info from new tables
g_numbering_plan = dict()

# basedir = os.path.abspath(os.path.dirname(__file__))
# config_dir = basedir + "/" + "config/"
# redis_cfg = config_dir + "redis.cfg"
db_host = config['postgresql']['host']
db_name = config['postgresql']['db']
db_user = config['postgresql']['user']
db_pass = config['postgresql']['password']

try:
    db = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_pass)
    db.autocommit = True
except Exception as error:
    logger.info(f"DB connection failed: {error}")

logger.info("DB connected")
cur = db.cursor()

#### redis ####
# def read_redis_config():
#     d = dict()
#     with open(redis_cfg, 'r',encoding='utf-8') as f:
#         lines = f.readlines()
#         for line in lines:
#             (k,v) = line.strip().split('=')
#             if k == 'host':
#                 d[k] = v
#             elif k == 'port' or k == 'db':
#                 d[k] = int(v)
#     return d

# d_redis = read_redis_config()
# for k,v in d_redis.items():
#     logger.info(f"redis param: {k} => {v} ({type(v)})")

# r = redis.Redis(host=f"{d_redis['host']}",port=f"{d_redis['port']}")
redis_host = config['redis']['host']
redis_port = config['redis']['port']

r = redis.Redis(host=redis_host,port=redis_port)

try:
    r.ping()
    logger.info("redis server connected")
except:
    logger.info("!!! Can not connect redis server, leave")
    exit()

### legacy code for authentication
# cur.execute("select h.customerid,c.name,h.api_key,h.api_secret_enc,h.salt,c.directory,c.currency from http_customers h,customers c where h.customerid=c.id;")
# rows = cur.fetchall()
# for row in rows:
#     (acid,acname,api_key,api_secret_enc,salt,dir,currency) = row
#     #logger.info(acid,api_key,api_secret_enc,salt,dir,currency)

#     g_userinfo[api_key]['secret_enc'] = api_secret_enc
#     g_userinfo[api_key]['salt'] = salt
#     g_userinfo[api_key]['name'] = acname
#     g_userinfo[acname]['dir'] = dir
#     g_userinfo[acname]['customerid'] = acid
#     g_userinfo[acname]['currency'] = currency

# for k,v in g_userinfo.items():
#     print(k,v)

cur.execute("""select a.api_key,a.api_secret,a.id as account_id, a.billing_id, b.company_name,a.product_id,
p.name as product_name,a.callback_url from account a join billing_account b on a.billing_id=b.id 
join product p on a.product_id=p.id where a.connection_type='http';""")
rows = cur.fetchall()
for row in rows:
    (api_key,api_secret,account_id,billing_id,company_name,product_id,product_name, callback_url) = row
    ac = {
        "api_key": api_key,
        "api_secret": api_secret,
        "account_id": account_id,
        "billing_id": billing_id,
        "company_name": company_name,
        "product_id": product_id,
        "product_name": product_name,
        "callback_url": callback_url
    }
    g_account[api_key] = ac

logger.info("### print all http account api credentials")
for api_key,ac in g_account.items():
    logger.info(f" - {api_key}")
    logger.info(json.dumps(ac, indent=4))
    
### select numbering plan 
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
        logger.warning("!!! problem fetching numbering_plan")
        return None

    return np

g_numbering_plan = get_numbering_plan(cur)
logger.info(f"### get_numbering_plan: {len(g_numbering_plan)} entries")