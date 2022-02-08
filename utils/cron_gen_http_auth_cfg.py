#!/usr/bin/python3
"""
~/utils/cron_gen_http_auth_cfg.py
generate ~/httpapi/.htaccess
"""
import os
from collections import defaultdict

basedir = os.path.abspath(os.path.dirname(__file__)) + "/../" #/home/amx
libdir = os.path.join(basedir, "pylib") #/home/amx/pylib
auth_file = os.path.join(basedir, ".htaccess")
#auth_file = os.path.join(basedir, "httpapi/.htaccess")
tmp_auth_file = auth_file + ".tmp"

import site
site.addsitedir(libdir)
import DB

db = DB.connectdb()
cur = db.cursor()

sql = """select api_key,api_secret, w.ipaddress from account a join whitelist_ip w on a.billing_id = w.billing_id 
        where a.connection_type='http' and a.live=1 and a.deleted=0;"""

cur.execute(sql)
rows = cur.fetchall()

d = defaultdict(list)
for row in rows:
    (api_key,api_secret,ip) = row

    index = f"{api_key}---{api_secret}"

    if not ip in d[index]:
        d[f"{api_key}---{api_secret}"].append(ip)

if len(d) > 0:
    with open(tmp_auth_file, 'w', encoding='utf8') as w:
        for index, l_ip in d.items():
            ips = ",".join(l_ip)
            w.write(f"{index}---{ips}\n")
    os.rename(tmp_auth_file,auth_file)
    print(f"{auth_file} is created")
else:
    print(f"nothing fetched from DB, don't rewrite {auth_file}")

cur.close()
db.close()
