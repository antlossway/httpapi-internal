#!/usr/bin/env python3
import random
import string
from uuid import uuid4
import re
import sys
import os

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

def gen_random_timestamp():
    now_epoch = time.time()
    random_epoch = time.time() - random.randint(0, 86400*10) #past 10 days
    time_obj = time.gmtime(random_epoch) #struct_time
    time_str = time.strftime("%Y-%m-%d, %H:%M:%S", time_obj)
    return time_str

base_bnumber = "+65"
country_id = 3

l_acname = ['ABC','XYZ']
l_sender = ['DBS', 'OCBC', 'Sephora', 'Apple']
content_template = ['<ID> is your verfication code', 'PIN CODE: <ID>']

basedir = os.path.abspath(os.path.dirname(__file__))
notif1_dir = os.path.join(basedir, "../notif1/")

def gen_xms():
    acname = random.choice(l_acname)
    bnumber = gen_bnumber(8)
    sender = random.choice(l_sender)
    msgid = str(uuid4())
    xms = gen_content()
    dcs = 0

    content = f"""\
; encoding=UTF-8
[{acname}]
Phone={bnumber}
OriginatingAddress={sender}
Priority=0
XMS={xms}
DCS={dcs}
LocalId={msgid}
MsgId={msgid}
"""
    xms_file = os.path.join(basedir, f"../sendxms/SERVER_SUPER100/received/{acname}/xms{msgid}")
    print(xms_file)

    with open(xms_file,'w') as w:
        w.write(content)


    ### create notif1 file
    notif1_filename = f"{acname}---{bnumber}---{msgid}---." + str(random.randint(0,10000))
    notif1_file = os.path.join(notif1_dir, f"{acname}/{notif1_filename}")
    print(f"create notif1 {notif1_file}")
    with open(notif1_file,'w') as f:
        pass
    
if __name__ == '__main__':
    num = int(sys.argv[1])
    for i in range(num):
        gen_xms()
