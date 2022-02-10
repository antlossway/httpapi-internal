#from textwrap import indent
#from typing import Optional,List
#from pydantic import BaseModel, Field
#from subprocess import call

from textwrap import indent
from fastapi import FastAPI, Body, Response, HTTPException, Depends, Request
from fastapi.responses import JSONResponse

import re
import smsutil
import random
from uuid import uuid4
import json
from email_validator import validate_email
from collections import defaultdict
import os
import requests

#import myutils
from myutils import logger, read_comma_sep_lines, gen_udh_base, gen_udh, generate_otp
import mysms
#from mysms import create_sms
from mydb import cur,r,g_account,g_numbering_plan
#import httpapi.myauth as myauth => does not work, saying there is no package httpapi
import myauth
import models

########################
### Global Variables ###
########################

min_len_msisdn = 10 #without prefix like 00 or +
max_len_msisdn = 15 #ITU-T recommendation E.164
max_len_tpoa = 11

redis_status_expire = 15*24*3600 # STATUS:<msgid1> => <status> for /sms/:msgid query_dlr

desc = ""

app = FastAPI(
    title="Internal CMI SMS API",
    description=desc,
    version="0.1.0",
    #terms_of_service="http://example.com/terms/",
    docs_url='/iapi/docs', 
    redoc_url='/iapi/redoc',
    openapi_url='/iapi/openapi.json'

)

    
def is_empty(field):
    if field == '' or field == None:
        return True
    return False

#@app.get('/')
#async def home():
#    return {'result': 'hello'}


@app.post('/iapi/internal/cpg') #UI get uploaded file from user, call this API to process data, if data valid will create campaign
#async def create_campaign(arg_new_cpg: models.InternalNewCampaign, request: Request, auth_result=Depends(myauth.allowinternal)):
async def create_campaign(
    request: Request,
    arg_new_cpg: models.InternalNewCampaign = Body(
                     ...,
                     examples=models.example_internal_cpg,
    ),
):

    # blast_list: List[str]
    # cpg_name: str
    # cpg_tpoa: str
    # cpg_xms: str
    # billing_id: int
    # account_id: int
    # product_id: int
    logger.info(f"{request.url.path}: from {request.client.host}")
    
    blast_list = arg_new_cpg.blast_list
    l_data = read_comma_sep_lines(blast_list)
    if not l_data: #None, means no valid bnumber
        raise HTTPException(status_code=422, detail=f"no valid entry")
    elif l_data == -1:
        raise HTTPException(status_code=422, detail=f"issue with the format of blast list content")
    else:
        cpg_name = arg_new_cpg.cpg_name
        tpoa = arg_new_cpg.cpg_tpoa
        xms = arg_new_cpg.cpg_xms
        billing_id = arg_new_cpg.billing_id
        account_id = arg_new_cpg.account_id
        product_id = arg_new_cpg.product_id

        cpg_name = re.sub(r"'",r"''",cpg_name)
        tpoa = re.sub(r"'",r"''",tpoa)
        xms = re.sub(r"'",r"''",xms)

        sql = f"""insert into cpg (name,tpoa,billing_id,account_id,product_id,xms) values 
                ('{cpg_name}','{tpoa}',{billing_id},{account_id},{product_id},'{xms}') returning id;"""
        logger.debug(sql)
        cur.execute(sql)
        cpg_id = cur.fetchone()[0]

        for d in l_data:
            hash_value = d.get('hash',None)
            if hash_value:
                del d['hash'] #delete 'hash' from the dict
                for k,v in d.items():
                    sql = f"""insert into cpg_blast_list (cpg_id,field_name,value,hash) values ({cpg_id}, '{k}','{v}','{hash_value}');"""
                    logger.debug(sql)
                    try:
                        cur.execute(sql)
                    except Exception as err:
                        logger.debug(f"!!! insertion error {err}")
        resp_json = {
            'cpg_id': cpg_id,
            'count_valid_entry': len(l_data)
        }

    logger.info("### reply UI:")
    logger.info(json.dumps(resp_json, indent=4))
 
    return JSONResponse(status_code=200, content=resp_json)


whitelist_ip = ['127.0.0.1','localhost','13.214.145.167']
@app.post('/iapi/internal/sms', response_model=models.SMSResponse, responses=mysms.example_create_sms_response)
async def internal_create_sms(arg_sms: models.InternalSMS, request:Request, auth_result=Depends(myauth.allowinternal)):
    d_sms = arg_sms.dict()
    logger.info(f"debug post body")
    logger.info(json.dumps(d_sms,indent=4))


   
    sender = arg_sms.sender #client may sent "from", which is alias as "sender"
    l_bnumber_in = d_sms.get("to", None).split(',') #comma separated bnumber for bulk process
    content = d_sms.get("content", None)

    l_bnumber = list() #to keep the final cleaned MSISDN
    for bnumber in l_bnumber_in:
        bnumber = mysms.clean_msisdn(bnumber)
        if bnumber:
            l_bnumber.append(bnumber)

    if len(l_bnumber) == 0:
        resp_json = {
            "errorcode": 1003,
            "errormsg": f"No valid B-number found"
        }
        return JSONResponse(status_code=422, content=resp_json)

    result = {}

    ### missing parameters
    if is_empty(sender) or is_empty(content):
        resp_json = {
            "errorcode": 1002,
            "errormsg": "missing parameter, please check if you forget 'from' or 'content'"
        }
        return JSONResponse(status_code=422, content=resp_json)

    ### sender format wrong
    len_sender = len(sender)
    if len_sender > max_len_tpoa:
        resp_json= {
            "errorcode": 1004,
            "errormsg": f"TPOA/Sender length should not be more than {max_len_tpoa} characters"
        }
        return JSONResponse(status_code=422, content=resp_json)

    ### optional param
    #require_dlr = arg_sms.status_report_req #default 1
    #orig_udh = arg_sms.udh #default None

    require_dlr = 0 # internal call don't need to return DLR

    ### get split info
    sms = smsutil.split(content)
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

    l_resp_msg = list() #list of dict

    for bnumber in l_bnumber:
        ### check B-number country/operator ###
        parse_result = mysms.parse_bnumber(g_numbering_plan,bnumber)
        if parse_result:
            country_id,operator_id = parse_result.split('---')

            for i,part in enumerate(sms.parts):
                xms = part.content
                msgid = str(uuid4())

                resp_msg = {"msgid": msgid, "to": bnumber}
                l_resp_msg.append(resp_msg)

                #if orig_udh != None and orig_udh != '':
                #    udh = orig_udh
                #    logger.info(f"keep orig UDH {udh}")

                #for long sms, our UDH will override orig UDH from client
                if udh_base != '':
                    udh = gen_udh(udh_base,split,i+1)
                    logger.debug(f"gen_udh: {udh}")

                data = {
                    "msgid": msgid,
                    "sender": sender,
                    "to": bnumber,
                    "content": xms,
                    "udh": udh,
                    "dcs": dcs
                    #"country_id": country_id,  ## qrouter will take care parse_bnumber for both smpp and http(again)
                    #"operator_id": operator_id,
                }

                if require_dlr == 0: #by default require_dlr=1,so no need to add
                    data["require_dlr"] = 0

         #        account = {
        #           "account_id": int
        #        }
                account = arg_sms.account.dict()
                account_id = account.get("account_id")
                ## check account is SMPP or HTTP
                sql = f"select connection_type, api_key, directory from account where id={account_id};"
                logger.info(sql)
                cur.execute(sql)
                errorcode = 0
                try:
                    row = cur.fetchone()
                    logger.info(row)
                    (conn_type,api_key,directory) = row
            
                    if conn_type == "smpp":
                        if os.path.isdir(directory) :
                            logger.info(f"smpp account, call internal_create_sms_smpp({directory},{json.dumps(data,indent=4)})")
                            errorcode = mysms.internal_create_sms_smpp(directory,data)
                        else:
                            logger.warning(f"{directory} is not yet created, wait for cronjob to creat it before making test")
                            resp_json = {
                                "errorcode": 5,
                                "errormsg": "SMPP directory not yet created, wait a few min before making test"
                            }
                            return JSONResponse(status_code=404, content=resp_json)
                    elif api_key:
                        acinfo = {
                            "api_key": api_key
                        }
                        logger.info(f"http account, call create_sms({acinfo},{json.dumps(data,indent=4)})")
                        errorcode = mysms.create_sms(acinfo,data)
                    else:
                        resp_json = {
                            "errorcode": 4,
                            "errormsg": "http account has no api_key, please contact support"
                        }
                        return JSONResponse(status_code=404, content=resp_json)

                except:
                    errorcode = 1

                if errorcode == 0:
                    pass
                else: #no need to process remain parts
                    resp_json = {
                        "errorcode": 6,
                        "errormsg": "Internal Server Error, please contact support"
                    }
                    #raise HTTPException(status_code=500, detail=f"Internal Server Error, please contact support")
                    return JSONResponse(status_code=422, content=resp_json)

                    break
        else:
            logger.warning(f"Receipient number {bnumber} does not belong to any network")


    resp_json = {
                 'errorcode': errorcode,
                 'message-count': len(l_resp_msg),
                 'messages': l_resp_msg
                }
    logger.info("### reply client:")
    logger.info(json.dumps(resp_json, indent=4))

    #return resp_json
    return JSONResponse(status_code=200, content=resp_json)


from werkzeug.security import generate_password_hash,check_password_hash

@app.post('/iapi/internal/login') #check webuser where deleted=0, and live=1
async def verify_login(arg_login: models.InternalLogin, request:Request, response:Response):
#async def verify_login(arg_login: models.InternalLogin, request:Request, response:Response, auth_result=Depends(myauth.allowinternal)):
    # check if username exists
    cur.execute("""select u.id as webuser_id,username,password_hash,email,bnumber,role_id,webrole.name as role_name,
    billing_id,b.billing_type,b.company_name,b.company_address,b.country,b.city,b.postal_code,b.currency from webuser u
        left join billing_account b on u.billing_id=b.id left join webrole on u.role_id=webrole.id where username=%s and u.deleted=0 and u.live=1;
        """, (arg_login.username,))
    row = cur.fetchone()
    if row:
        (webuser_id,username,password_hash,email,bnumber,role_id,role_name,billing_id,billing_type,company_name,company_address,
        country,city,postal_code,currency) = row
        ##verify password
        #if arg_login.password_hash == password_hash:
        if check_password_hash(password_hash,arg_login.password):
            resp_json = {
                "errorcode":0,
                "status":"Success",
                "id":webuser_id,
                "username":username,
                "email":email,
                "bnumber":bnumber,
                "role_id":role_id,
                "role":role_name,
                "billing_id":billing_id,
                "billing_type":billing_type,
                "company_name":company_name,
                "company_address":company_address,
                "country":country,
                "city":city,
                "postal_code":postal_code,
                "currency":currency
            }
        else:
            resp_json = {
                'errorcode': 1,
                'status': "Wrong password!"
            }
            response.status_code = 401

    else:
        resp_json = {
            'errorcode': 1,
            'status': "User not found!"
        }
        response.status_code = 401

    logger.info("### reply internal UI:")
    logger.info(json.dumps(resp_json, indent=4))

    return JSONResponse(status_code=200, content=resp_json)

@app.get("/iapi/internal/billing") # get all billing accounts
async def get_all_billing_accounts():
    cur.execute(f"""
    select id,company_name,company_address,country,city,postal_code,contact_name,billing_email,
    contact_number,billing_type,currency,live,ip_list from billing_account where id != 4 and deleted=0;""")

    l_data = list()
    rows = cur.fetchall()
    for row in rows:
        (billing_id,company_name,company_address,country,city,postal_code,contact_name,billing_email,
        contact_number,billing_type,currency,live,ip_list) = row
        d = {
            "billing_id": billing_id,
            "company_name": company_name,
            "company_address": company_address,
            "country": country,
            "city": city,
            "postal_code": postal_code,
            "contact_name": contact_name,
            "billing_email": billing_email,
            "contact_number": contact_number,
            "billing_type": billing_type,
            "currency": currency,
            "live": live,
            "ip_list": ip_list
        }
        l_data.append(d)
    
    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "status": "Success",
            "count": len(l_data),
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":f"Account Not found"
        }
        return JSONResponse(status_code=404, content=resp_json)

    logger.info("### reply internal UI:")
    logger.info(json.dumps(resp_json, indent=4))
 
    return JSONResponse(status_code=200, content=resp_json)

@app.get("/iapi/internal/billing/{billing_id}") # get billing account info
async def get_billing_account_info(billing_id: int):
    cur.execute(f"""
    select id,company_name,company_address,country,city,postal_code,contact_name,billing_email,
    contact_number,billing_type,currency,live,ip_list from billing_account where deleted=0 and id=%s""",(billing_id,))

    try:
        row = cur.fetchone()
        (billing_id,company_name,company_address,country,city,postal_code,contact_name,billing_email,
        contact_number,billing_type,currency,live,ip_list) = row
        resp_json = {
            "billing_id": billing_id,
            "company_name": company_name,
            "company_address": company_address,
            "country": country,
            "city": city,
            "postal_code": postal_code,
            "contact_name": contact_name,
            "billing_email": billing_email,
            "contact_number": contact_number,
            "billing_type": billing_type,
            "currency": currency,
            "live": live,
            "ip_list": ip_list
        }
#        print(resp_json)
    except:
        resp_json = {
            "errorcode": 1,
            "status":"Users Not found!"
        }
        return JSONResponse(status_code=404, content=resp_json)

    logger.info("### reply internal UI:")
    logger.info(json.dumps(resp_json, indent=4))
 
    return JSONResponse(status_code=200, content=resp_json)


# use responses to add additional response like returning errors
@app.get("/iapi/internal/account/{billing_id}") #get all accounts for a billing account
def get_accounts_by_billing_id(billing_id: int):
    result = func_get_all_accounts(billing_id)
    return result


@app.get("/iapi/internal/account")#get all accounts (related to billing accounts)
def get_all_accounts():
    result = func_get_all_accounts()
    return result


def func_get_all_accounts(billing_id=None):
    sql = """select a.billing_id,b.company_name,a.id as account_id,a.name as account_name,a.product_id,p.name as product_name,a.live,
    a.connection_type, a.systemid,a.password,a.api_key,a.api_secret,a.callback_url,a.comment from account a join billing_account b on b.id=a.billing_id 
    join product p on a.product_id = p.id where a.deleted=0"""

    if billing_id:
        sql += f"and a.billing_id={billing_id};"
    logger.info(sql)
    cur.execute(sql)

    l_data = list() #list of dict
    rows = cur.fetchall()
    for row in rows:
        (billing_id,company_name,account_id,account_name,product_id,product_name,live,connection_type,systemid,password,api_key,api_secret,callback_url,comment) = row
        d = {
            "billing_id": billing_id,
            "company_name": company_name,
            "account_id": account_id,
            "account_name": account_name,
            "product_id": product_id,
            "product_name": product_name,
            "live": live,
            "connection_type": connection_type,
            "systemid": systemid,
            "password": password,
            "api_key": api_key,
            "api_secret": api_secret,
            "callback_url": callback_url,
            "comment": comment,
        }
        l_data.append(d)
    
    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "status": "Success",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"Account Not found!"
        }
        return JSONResponse(status_code=404, content=resp_json)

    logger.info("### reply internal UI:")
    logger.info(json.dumps(resp_json, indent=4))
 
    return JSONResponse(status_code=200, content=resp_json)

@app.get("/iapi/internal/webuser")#get all webusers
def get_all_webusers():
    result = func_get_webusers()
    return result

@app.get("/iapi/internal/webuser/{billing_id}")#get all webuser of one billing account
def get_webusers_by_billing_id(billing_id:int):

    result = func_get_webusers(billing_id)
    return result

def func_get_webusers(billing_id=None):
    sql = f"""select u.id as webuser_id,u.username,u.email,u.bnumber,b.company_name,u.role_id,r.name as role_name,
    u.live from webuser u join billing_account b on u.billing_id=b.id join webrole r on r.id=u.role_id 
    where u.deleted=0"""
    
    if billing_id:
        sql += f" and u.billing_id={billing_id};"
    cur.execute(sql)
    logger.info(sql)

    l_data = list() #list of dict
    rows = cur.fetchall()
    for row in rows:
        (webuser_id,username,email,bnumber,company_name,role_id,role_name,live) = row
        d = {
            "webuser_id": webuser_id,
            "username": username,
            "email": email,
            "bnumber": bnumber,
            "company_name": company_name,
            "role_id": role_id,
            "role_name": role_name,
            "live": live
        }
        l_data.append(d)
    
    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "status": "Success",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"Webuser Not found!"
        }
        return JSONResponse(status_code=404, content=resp_json)

    logger.info("### reply internal UI:")
    logger.info(json.dumps(resp_json, indent=4))
  
    return JSONResponse(status_code=200, content=resp_json)

def get_userid_from_username(username):
    cur.execute("select id from webuser where username=%s",(username,))
    try:
        webuser_id = cur.fetchone()[0]
        return webuser_id
    except:
        return None

def get_userid_from_email(email):
    cur.execute("select id from webuser where email=%s",(email,))
    try:
        webuser_id = cur.fetchone()[0]
        return webuser_id
    except:
        return None

@app.post("/iapi/internal/insert", 
#response_model=models.InsertResponse, 
            responses={404: {"errorcode": 1, "status": "some error msg"} }
)
async def insert_record(
    args: models.InternalInsert = Body(
                     ...,
                     examples=models.example_internal_insert,
    ),
    #request: Request
):
    d_args = args.dict()
    logger.debug(f"### orig internal insert request body: {json.dumps(d_args, indent=4)}")

    if not 'table' in d_args:
        resp_json = {
            "errorcode":2,
            "status": f"missing compulsory field table"
        }
        return JSONResponse(status_code=500,content=resp_json)

    table = d_args['table']
    #del d_args['table']

    if table == 'billing_account':
        #company_name and contact name is compulsory
        try:
            data_obj = models.InsertBillingAccount(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field company_name or contact-name"
            }
            return JSONResponse(status_code=500,content=resp_json)
        
        # if billing_email is provided, check if email is valid, comma separated email, will not check uniqueness of email
        if data_obj.billing_email: #email not null
            emails = data_obj.billing_email.split(',')
            for email in emails:
                try:
                    valid = validate_email(email) # return a email object
                except:
                    resp_json = {
                        "errorcode":1,
                        "status": f"Incorrect email address {email}"
                    }
                    return JSONResponse(status_code=422,content=resp_json)
                    break
            
    elif table == 'webuser': 
            ## compulsory field
            # username: str
            # ## optional field
            # password_hash: Optional[str]
            # email: Optional[int]
            # billing_id: Optional[int]
            # role_id: Optional[int]
            # bnumber: Optional[str]     
        try:
            data_obj = models.InsertWebUser(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)
        ### username and email should be unique
        username = data_obj.username
        email = data_obj.email
        if username and get_userid_from_username(username):
            resp_json = {
                "errorcode":2,
                "status": f"username {username} exists"
            }
            return JSONResponse(status_code=403,content=resp_json)
        elif email and get_userid_from_email(email):
            resp_json = {
                "errorcode":2,
                "status": f"email {email} exists"
            }
            return JSONResponse(status_code=403,content=resp_json)

    elif table == 'audit': 
            ## compulsory field
            # billing_id: int
            # webuser_id: int
            # auditlog: st    
        try:
            data_obj = models.InsertAudit(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)
#    elif table == 'whitelist_ip': 
#            ## compulsory field
#            # billing_id: int
#            # webuser_id: int
#            # ipaddress: str    
#        try:
#            data_obj = models.InsertWhitelistIP(**args.dict()) #convert into defined model, removing useless field
#        except:
#            resp_json = {
#                "errorcode":2,
#                "status": f"missing compulsory field"
#            }
#            return JSONResponse(status_code=500,content=resp_json)
    elif table == 'account':
        ##compulsory field
        #billing_id: int
        #name: str
        #product_id: int
        #connection_type: smpp/http
        conn_type = d_args.get('connection_type')
        if not conn_type:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field connection_type"
            }
            return JSONResponse(status_code=500,content=resp_json)
        if conn_type == 'smpp':
            try:
                data_obj = models.InsertSMPPAccount(**args.dict()) #convert into defined model, removing useless field
            except:
                resp_json = {
                    "errorcode":2,
                    "status": f"missing compulsory field"
                }
                return JSONResponse(status_code=500,content=resp_json)
        else:
            try:
                data_obj = models.InsertHTTPAccount(**args.dict()) #convert into defined model, removing useless field
            except:
                resp_json = {
                    "errorcode":2,
                    "status": f"missing compulsory field"
                }
                return JSONResponse(status_code=500,content=resp_json)

        name = data_obj.name.strip() #smpp_account.name should be unique
        ## remove any special char, replace space with _
        name = re.sub(r'\s',r'_', name) #abc xyz => abc_xyz
        existing_id = None
        cur.execute("select id from account where name=%s", (name,))
        try:
            existing_id = cur.fetchone()[0]
        except:
            pass

        if existing_id:
            resp_json = {
                "errorcode":2,
                "status": f"account name {name} exists"
            }
            return JSONResponse(status_code=403,content=resp_json)

        data_obj.name = name #put back cleaned name into object

    elif table == 'selling_price': 
            ## compulsory field
            # account_id: int
            # country_id: int
            # operator_id: int
            # selling_price: float
            # validity_date: str
            # admin_webuser_id: int
        try:
            data_obj = models.InsertSellingPrice(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)

        account_id = data_obj.account_id
        country_id = data_obj.country_id
        operator_id = data_obj.operator_id
        validity_date = data_obj.validity_date
        #make sure uniq entry for each validity_date
        cur.execute("""select id,price from selling_price where and account_id=%s and country_id=%s and operator_id=%s
                        and validity_date=%s""", (account_id,country_id,operator_id,validity_date))
        try:
            (existing_id,price) = cur.fetchone()
            if existing_id:
                resp_json = {
                    "errorcode":2,
                    "status": f"already have price {price} defined for validity_date {validity_date}"
                }
                return JSONResponse(status_code=403,content=resp_json)
        except:
            pass
 
#
    #### general processing for any table
    d_data = data_obj.dict()

    if table == 'account' and conn_type == 'smpp': # generate systemid/password/directory/notif3_dir
        name = d_data.get('name')
        ## create directory, notif_dir
        ext = generate_otp('lower',4) #give a random extension to avoid same subdir name, e.g abc4567
        systemid = name[:12]
        systemid = f"{re.sub(r'_$','',systemid)}_{ext}"
        subdir = systemid.upper()
        basedir = os.path.abspath(os.path.dirname(__file__))
        directory = os.path.join(basedir, f"../sendxms/SERVER_SUPER100/received/{subdir}") #/home/amx/sendxms/SERVER_SUPER100
        notif3_dir = os.path.join(basedir, f"../sendxms/SERVER_SUPER100/spool/{subdir}")
        d_data['directory'] = directory
        d_data['notif3_dir'] = notif3_dir 
        ## create systemid, password
        password = generate_otp('alphanumeric',8)

        d_data['systemid'] = systemid
        d_data['password'] = password
        logger.info(f"debug smpp_account: {json.dumps(d_data,indent=4)}")
    elif table == 'account' and conn_type == 'http': # generate api_key/api_secret
        api_key = generate_otp('alphanumeric',20)
        api_secret = generate_otp('alphanumeric',40)
        d_data['api_key'] = api_key
        d_data['api_secret'] = api_secret


    data = dict() #hold the fields to be inserted into destination table
    
    fields,values = '', ''
    for k,v in d_data.items():
        if not v is None:
            data[k] = v
            fields += f"{k},"
            if isinstance(v, (int, float)): #is a number
                values += f"{v},"
            else:
                v = re.sub(r"'", "''",v) ##replace single quote ' with ''
                values += f"'{v}',"

    logger.debug(f"### after formatting and removing null value: {json.dumps(data,indent=4)}")

    fields = fields[:-1]
    values = values[:-1]

    sql = f"insert into {table} ({fields}) values ({values}) returning id;"
    logger.debug(sql)
    ### insert into table
    try:
        # new_id = cur.execute("""insert into billing_account (company_name,contact_name,billing_type,company_address,country,
        # city,postal_code,billing_email) values (%s,%s,%s,%s,%s,%s,%s,%s) returning id""",
        # (data['company_name'],data['contact_name'],data['billing_type'],data['company_address'],data['country'],data['city'],data['postal_code'],data['billing_email'])
        # )
        new_id = cur.execute(sql)
        try: 
            new_id = cur.fetchone()[0]
            if new_id:
                resp_json = {
                    "errorcode":0,
                    "status": "Success",
                    "id": new_id,
                    "result": data
                }

                ## update all qrouter
                if table == "account":
                    logger.info("#### processctl update allqrouter ###")
                    try:
                        result = os.system("/home/amx/script/processctl.pl update allqrouter")
                        if result == 0:
                            logger.info("update allqrouter successful")
                        else:
                            logger.warning("update allqrouter failed")
                    except:
                        pass
        except Exception as err:
            resp_json = {
                "errorcode":2,
                "status": f"insert {table} failure, no new id returned: {err}"
            }
            logger.info(f"reply internal insert: {json.dumps(resp_json,indent=4)}")
            return JSONResponse(status_code=500, content=resp_json)

    except Exception as err:
        resp_json = {
            "errorcode":2,
            "status": f"insert {table} failure: {err}"
        }
        logger.info(f"reply internal insert: {json.dumps(resp_json,indent=4)}")

        #raise HTTPException(status_code=500, detail={"errocode": 2, "status": f"insert DB error: {err}"})
        return JSONResponse(status_code=500, content=resp_json)      
    
    logger.info("### reply internal insert: {json.dumps(resp_json,indent=4)}")
    return JSONResponse(status_code=200,content=resp_json)



@app.post("/iapi/internal/update", 
#response_model=models.InsertResponse, 
            responses={404: {"errorcode": 1, "status": "some error msg"} }
)
async def update_record(
    args: models.InternalUpdate = Body(
                     ...,
                     examples=models.example_internal_update,
    ),
    #request: Request
):
    d_args = args.dict()
    logger.debug(f"### orig internal update request body: {json.dumps(d_args, indent=4)}")

    if not 'table' in d_args or not 'id' in d_args:
        resp_json = {
            "errorcode":2,
            "status": f"missing compulsory field table or id"
        }
        return JSONResponse(status_code=500,content=resp_json)

    table = d_args['table']
    id = d_args['id']

    if table == 'billing_account':
        try:
            data_obj = models.UpdateBillingAccount(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)
        
        # if billing_email is provided, check if email is valid, comma seprated email
        if data_obj.billing_email: #email not null
            emails = data_obj.billing_email.split(',')
            for email in emails:
                try:
                    valid = validate_email(email) # return a email object
                except:
                    resp_json = {
                        "errorcode":1,
                        "status": f"Incorrect email address {email}"
                    }
                    return JSONResponse(status_code=422,content=resp_json)
                    break

    elif table == 'webuser': 
        try:
            data_obj = models.UpdateWebUser(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)
        ### username and email should be unique
        username = data_obj.username
        email = data_obj.email
        existing_id_username = get_userid_from_username(username)
        existing_id_email = get_userid_from_username(email)
        if username and existing_id_username and existing_id_username != id:
            resp_json = {
                "errorcode":2,
                "status": f"username {username} exists"
            }
            return JSONResponse(status_code=403,content=resp_json)
        elif email and existing_id_email and existing_id_email != id:
            resp_json = {
                "errorcode":2,
                "status": f"email {email} exists"
            }
            return JSONResponse(status_code=403,content=resp_json)
#    elif table == 'whitelist_ip':
#        try:
#            data_obj = models.UpdateWhitelistIP(**args.dict()) #convert into defined model, removing useless field
#        except:
#            resp_json = {
#                "errorcode":2,
#                "status": f"missing compulsory field"
#            }
#            return JSONResponse(status_code=500,content=resp_json)
    elif table == 'account':
        try:
            data_obj = models.UpdateAccount(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)
    elif table == 'selling_price':
        try:
            data_obj = models.UpdateSellingPrice(**args.dict()) #convert into defined model, removing useless field
        except:
            resp_json = {
                "errorcode":2,
                "status": f"missing compulsory field"
            }
            return JSONResponse(status_code=500,content=resp_json)

        ## debug
        #d_data = data_obj.dict()
        #print(json.dumps(d_data, indent=4))

        validity_date = data_obj.validity_date
        #make sure uniq entry for each validity_date
        if validity_date:
            # get billing_id,account_id of the specified entry
            cur.execute(f"select id,account_id,country_id,operator_id from selling_price where id={id}")
            (id,account_id,country_id,operator_id) = cur.fetchone()

            # check if there is existing validity_date for the same account
            cur.execute("""select id,price from selling_price where account_id=%s and country_id=%s and operator_id=%s
                        and validity_date=%s and id != %s""", (account_id,country_id,operator_id,validity_date,id))
            try:
                (existing_id,price) = cur.fetchone()
                if existing_id:
                    resp_json = {
                        "errorcode":2,
                        "status": f"already have price {price} defined for validity_date {validity_date}"
                    }
                    return JSONResponse(status_code=403,content=resp_json)
            except:
                pass

    #### general processing for any table
    d_data = data_obj.dict()
    
    data = dict() #hold the fields to be updated to destination table
    
    set_cmd = ''
    for k,v in d_data.items():
        if not v is None:
            data[k] = v
            if isinstance(v, (int, float)): #is a number
                set_cmd += f"{k}={v},"
            else:
                v = re.sub(r"'", "''",v) ##replace single quote ' with ''
                set_cmd += f"{k}='{v}',"

    logger.debug(f"### after formatting and removing null: {json.dumps(data,indent=4)}")

    set_cmd = set_cmd[:-1] #remove ending ,

    sql = f"update {table} set {set_cmd},update_time=current_timestamp where id={id} returning id;"
    logger.debug(sql)
    ### insert into table
    try:
        new_id = cur.execute(sql)
        try: 
            new_id = cur.fetchone()[0]
            if new_id:
                resp_json = {
                    "errorcode":0,
                    "status": "Success",
                    "id": new_id,
                    "result": data
                }
                logger.debug(f"### reply internal update: {json.dumps(resp_json,indent=4)}")

        except Exception as err:
            resp_json = {
                "errorcode":2,
                "status": f"update {table} failed, no id returned: {err}"
            }
            logger.info(f"reply internal update: {json.dumps(resp_json,indent=4)}")
            return JSONResponse(status_code=500, content=resp_json)

    except Exception as err:
        resp_json = {
            "errorcode":2,
            "status": f"update {table} failed: {err}"
        }
        logger.info(f"reply internal update: {json.dumps(resp_json,indent=4)}")
        return JSONResponse(status_code=500, content=resp_json)      
    
    return JSONResponse(status_code=200,content=resp_json)

@app.post("/iapi/internal/delete", 
            responses={404: {"errorcode": 1, "status": "some error msg"} }
)
async def delete_record(
    args: models.InternalDelete
):
    d_args = args.dict()
    logger.debug(f"### orig internal delete request body: {json.dumps(d_args, indent=4)}")

    if not 'table' in d_args or not 'id' in d_args:
        resp_json = {
            "errorcode":2,
            "status": f"missing compulsory field table or id"
        }
        return JSONResponse(status_code=500,content=resp_json)

    table = d_args['table']
    id = d_args['id']

    #### general processing for any table
    sql = f"delete from {table} where id={id} returning id;"
    logger.debug(sql)
    resp_json = dict()
    try:
        cur.execute(sql)
        cnt = cur.rowcount
        if cnt:
            resp_json = {
                "errorcode":0,
                "status": "Success",
                "id": id,
                "result": "deleted"
            }
            logger.debug(f"### reply internal delete: {json.dumps(resp_json,indent=4)}")
        else:
            resp_json = {
                "errorcode":2,
                "status": f"no id {id} found in {table}",
            }
            logger.debug(f"### reply internal delete: {json.dumps(resp_json,indent=4)}")
            return JSONResponse(status_code=404, content=resp_json)

    except Exception as err:
        resp_json = {
            "errorcode":2,
            "status": f"delete {table} failed: {err}"
        }
        logger.info(f"reply internal delete: {json.dumps(resp_json,indent=4)}")
        return JSONResponse(status_code=500, content=resp_json)      
    
    return JSONResponse(status_code=200,content=resp_json)


@app.post("/iapi/internal/password_hash")
async def get_password_hash(args: models.PasswordHashRequest):
    password_hash = generate_password_hash(args.password)
    resp_json = {
        "password": args.password,
        "password_hash": password_hash
    }
    return JSONResponse(content=resp_json)

@app.get("/iapi/internal/audit")
async def get_auditlog():
    cur.execute(f"""select a.creation_time,u.username,a.auditlog,b.company_name from audit a 
                join webuser u on a.webuser_id = u.id join billing_account b on u.billing_id = b.id order by a.creation_time desc limit 100;""")

    rows = cur.fetchall()
    l_data = list()
    for row in rows:
        (ts, username,auditlog,company_name) = row
        ts = ts.strftime("%Y-%m-%d, %H:%M:%S") #convert datetime.datetime obj to string
        d = {
            "timestamp": ts,
            "username": username,
            "audit": auditlog,
            "company_name": company_name
        }
        l_data.append(d)
    
    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "status": "Success",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"Auditlog Not found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    return JSONResponse(status_code=200, content=resp_json)


@app.get("/iapi/internal/audit/{billing_id}", response_model=models.GetAuditResponse,
        responses={404: {"model": models.MsgNotFound}})
async def get_auditlog_by_billing_id(billing_id:int):
    cur.execute(f"""select a.creation_time,u.username,a.auditlog,b.company_name from audit a 
                join webuser u on a.webuser_id = u.id join billing_account b on u.billing_id = b.id where u.billing_id={billing_id} order by a.creation_time desc limit 100;""")

    rows = cur.fetchall()
    l_data = list()
    for row in rows:
        (ts, username,auditlog,company_name) = row
        ts = ts.strftime("%Y-%m-%d, %H:%M:%S") #convert datetime.datetime obj to string
        d = {
            "timestamp": ts,
            "username": username,
            "audit": auditlog,
            "company_name": company_name
        }
        l_data.append(d)
    
    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "status": "Success",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "errormsg":"Auditlog Not found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    return JSONResponse(status_code=200, content=resp_json)


@app.post("/iapi/internal/traffic_report") #optional arg: billing_id, account_id
async def traffic_report(
    args: models.TrafficReportRequest = Body(
        ...,
        examples = models.example_traffic_report_request,
    ),
):
    d_arg = args.dict()
    billing_id = d_arg.get("billing_id")
    account_id = d_arg.get("account_id")
    start_date = d_arg.get("start_date",None)
    end_date = d_arg.get("end_date",None)
    if not start_date or not end_date: #default return past 7 days traffic
        sql = f"""select date, b.company_name,a.name as account_name,p.name as product_name,countries.name as country,
        status,sum(sum_split),sum(sum_sell) from cdr_agg join billing_account b on cdr_agg.billing_id=b.id join account a on cdr_agg.account_id=a.id 
        join product p on cdr_agg.product_id=p.id join countries on cdr_agg.country_id=countries.id where date >= current_date - interval '7 days' """

    else:
        sql = f"""select date, b.company_name,a.name as account_name,p.name as product_name,countries.name as country,
        status,sum(sum_split),sum(sum_sell) from cdr_agg join billing_account b on cdr_agg.billing_id=b.id join account a on cdr_agg.account_id=a.id 
        join product p on cdr_agg.product_id=p.id join countries on cdr_agg.country_id=countries.id where date between '{start_date}' and '{end_date}' """

    if account_id:
        sql += f"and cdr_agg.account_id = {account_id}"
    elif billing_id:
        sql += f"and cdr_agg.billing_id = {billing_id}"
    sql += "group by date,company_name,account_name,product_name,countries.name,status order by date"
    logger.info(sql)

    l_data = list()
    data_qty = defaultdict(dict) #2-dimention dict with sub-dict status => qty
    data_sell = defaultdict(float) #simple dict
    final_total_qty, final_total_sell = 0,0

    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        (day,company_name,account_name,product_name,country,status,qty,sell) = row
        day = day.strftime("%Y-%m-%d")
        if not status or status == '':
            status = 'Pending'
        try:
            data_qty[f"{day}---{company_name}---{account_name}---{product_name}---{country}"][status] += qty
        except:
            data_qty[f"{day}---{company_name}---{account_name}---{product_name}---{country}"][status] = qty

        data_sell[f"{day}---{company_name}---{account_name}---{product_name}---{country}"] += sell

        final_total_qty += qty
        final_total_sell += sell

    for key,d_status_qty in sorted(data_qty.items()):
        day,company_name,account_name,product_name,country = key.split('---')
        d = dict()
        total_qty_per_country = 0

        for status,qty in d_status_qty.items(): # for data_qty: status => qty, for data_sell: status => sell
            d[status] = qty
            total_qty_per_country += qty
        d['date'] = day
        d['company_name'] = company_name
        d['account_name'] = account_name
        d['product_name'] = product_name
        d['country'] = country
        d['total_sent'] = total_qty_per_country
#        d['cost'] = total * 0.01 #TBD: get_selling_pricing
        d['cost'] = f"{data_sell.get(key,0):,.3f}"
        l_data.append(d)
    
    if len(l_data) > 0:
        resp_json = {
            "errorcode" : 0,
            "status": "Success",
            "count": len(l_data),
            "total_qty": f"{final_total_qty:,}",
            "total_cost": f"{final_total_sell:,.3f}",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No Record found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    return JSONResponse(status_code=200, content=resp_json)
    
@app.post("/iapi/internal/transaction") #optional arg: billing_id, account_id
async def transaction_report(
    args: models.TransactionRequest = Body(
        ...,
        examples = models.example_transaction_report_request,
    ),
):
    d_arg = args.dict()
    msgid = d_arg.get("msgid")
    bnumber = d_arg.get("bnumber")
    billing_id = d_arg.get("billing_id")
    account_id = d_arg.get("account_id")
    start_date = d_arg.get("start_date",None)
    end_date = d_arg.get("end_date",None)

    if msgid:
        sql = f"""select cdr.dbtime,billing_account.company_name,account.name as account_name,cdr.msgid,cdr.tpoa,cdr.bnumber,countries.name as country,operators.name as operator,
                cdr.status,cdr.xms,cdr.udh,cdr.split,to_char(notif3_dbtime,'YYYY-MM-DD HH24:MI:SS') as notif_dbtime from cdr 
                join billing_account on cdr.billing_id=billing_account.id join account on cdr.account_id=account.id 
                join countries on cdr.country_id=countries.id join operators on cdr.operator_id=operators.id where cdr.msgid='{msgid}' """
    else:
        if not start_date or not end_date: #default return past 7 days traffic
            sql = f"""select cdr.dbtime,billing_account.company_name,account.name as account_name,cdr.msgid,cdr.tpoa,cdr.bnumber,countries.name as country,operators.name as operator,
                    cdr.status,cdr.xms,cdr.udh,cdr.split,to_char(notif3_dbtime,'YYYY-MM-DD HH24:MI:SS') as notif_dbtime from cdr 
                    join billing_account on cdr.billing_id=billing_account.id join account on cdr.account_id=account.id 
                    join countries on cdr.country_id=countries.id join operators on cdr.operator_id=operators.id where cdr.dbtime > current_timestamp - interval '7 days' """
        else:
            sql = f"""select cdr.dbtime,billing_account.company_name,account.name as account_name,cdr.msgid,cdr.tpoa,cdr.bnumber,countries.name as country,operators.name as operator,
                    cdr.status,cdr.xms,cdr.udh,cdr.split,to_char(notif3_dbtime,'YYYY-MM-DD HH24:MI:SS') as notif_dbtime from cdr 
                    join billing_account on cdr.billing_id=billing_account.id join account on cdr.account_id=account.id 
                    join countries on cdr.country_id=countries.id join operators on cdr.operator_id=operators.id where date(cdr.dbtime) between '{start_date}' and '{end_date}' """
    
        if account_id:
            sql += f"and cdr.account_id = {account_id}"
        elif billing_id:
            sql += f"and cdr.billing_id = {billing_id}"

        if bnumber:
            bnumber = mysms.clean_msisdn(bnumber)
            sql += f"and cdr.bnumber = '{bnumber}'"
    
        sql += "order by dbtime desc limit 100;"""

    logger.info(sql)
    
    l_data = list()
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        (ts,company_name,account_name,msgid,tpoa,bnumber,country,operator,status,xms,udh,split,notif3_dbtime) = row
        ts = ts.strftime("%Y-%m-%d, %H:%M:%S") #convert datetime.datetime obj to string
        d = {
            "timestamp": ts,
            "company_name": company_name,
            "account_name": account_name,
            "msgid": msgid,
            "tpoa": tpoa,
            "bnumber": bnumber,
            "country": country,
            "operator": operator,
            "status": status,
            "xms": xms,
            "udh": udh,
            "split": 1,
            "cost": 0.01,
            "notif3_dbtime": notif3_dbtime
        }

        l_data.append(d)

    if len(l_data) > 0:
        resp_json = {
            "errorcode" : 0,
            "status": "Success",
            "count": len(l_data),
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No Record found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    return JSONResponse(status_code=200, content=resp_json)

@app.post("/iapi/internal/volume_chart") #optional arg: billing_id, account_id
async def volume_chart(
    args: models.TrafficReportRequest = Body(
        ...,
        examples = models.example_traffic_report_request,
    ),
):
    d_arg = args.dict()
    billing_id = d_arg.get("billing_id")
    account_id = d_arg.get("account_id")
    start_date = d_arg.get("start_date",None)
    end_date = d_arg.get("end_date",None)
    if not start_date or not end_date: #default return past 7 days traffic
        sql = f"""select date,b.company_name,sum(sum_split) from cdr_agg join billing_account b on cdr_agg.billing_id = b.id where date >= current_date - interval '7 days' """

    else:
        sql = f"""select date,b.company_name,sum(sum_split) from cdr_agg join billing_account b on cdr_agg.billing_id = b.id where date between '{start_date}' and '{end_date}' """

    if account_id:
        sql += f"and account_id = {account_id}"
    elif billing_id:
        sql += f"and billing_id = {billing_id}"
    sql += "group by date,b.company_name order by date"
    logger.info(sql)

    l_data = list()

    d1 = defaultdict(list) #company_name => [ day1---qty1, day2---qty2...]

    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        (day,company_name,qty) = row
        day = day.strftime("%Y-%m-%d")

        d1[company_name].append(f"{day}---{qty}")
   
    for company_name,l_v in sorted(d1.items()):
        l1 = list()
        for v in l_v:
            day,qty = v.split('---')
            d = {
                "x": day,
                "y": qty
            }

            l1.append(d)

        d_company = {
            "name": company_name,
            "data": l1
        }

        l_data.append(d_company)
    
    
    if len(l_data) > 0:
        resp_json = {
            "errorcode" : 0,
            "status": "Success",
            "count": len(l_data),
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No Record found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    return JSONResponse(status_code=200, content=resp_json)

@app.post("/iapi/internal/cost_chart") #optional arg: billing_id, account_id
async def volume_chart(
    args: models.TrafficReportRequest = Body(
        ...,
        examples = models.example_traffic_report_request,
    ),
):
    d_arg = args.dict()
    billing_id = d_arg.get("billing_id")
    account_id = d_arg.get("account_id")
    start_date = d_arg.get("start_date",None)
    end_date = d_arg.get("end_date",None)
    if not start_date or not end_date: #default return past 7 days traffic
        sql = f"""select date,b.company_name,sum(sum_sell) from cdr_agg join billing_account b on cdr_agg.billing_id = b.id where date >= current_date - interval '7 days' """

    else:
        sql = f"""select date,b.company_name,sum(sum_sell) from cdr_agg join billing_account b on cdr_agg.billing_id = b.id where date between '{start_date}' and '{end_date}' """

    if account_id:
        sql += f"and account_id = {account_id}"
    elif billing_id:
        sql += f"and billing_id = {billing_id}"
    sql += "group by date,b.company_name order by date"
    logger.info(sql)

    l_data = list()
    d1 = defaultdict(list) #company_name => [ day1---qty1, day2---qty2...]

    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        (day,company_name,qty) = row #qty = sell
        day = day.strftime("%Y-%m-%d")
        qty = round(float(qty) , 3) 

        d1[company_name].append(f"{day}---{qty}")
   
    for company_name,l_v in sorted(d1.items()):
        l1 = list()
        for v in l_v:
            day,qty = v.split('---')
            d = {
                "x": day,
                "y": qty
            }

            l1.append(d)
        d = {
            "name": company_name,
            "data": l1
        }

        l_data.append(d)
    
    
    if len(l_data) > 0:
        resp_json = {
            "errorcode" : 0,
            "status": "Success",
            "count": len(l_data),
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No Record found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    return JSONResponse(status_code=200, content=resp_json)

#@app.post("/iapi/internal/cpg_report") #optional arg: billing_id, account_id
#async def campaign_report(
#    args: models.TrafficReportRequest = Body(
#        ...,
#        examples = models.example_traffic_report_request,
#    ),
#):
#    d_arg = args.dict()
#    billing_id = d_arg.get("billing_id")
#    account_id = d_arg.get("account_id")
#    start_date = d_arg.get("start_date",None)
#    end_date = d_arg.get("end_date",None)
#    if not start_date or not end_date: #default return all campaign
#
#        sql = f"""select n.cpg_id,cpg.name,cpg.status,cpg.creation_time,cpg.sending_time,cpg.tpoa,b.company_name,a.name as account_name,p.name as product_name,cpg.xms,count(*) from cpg_blast_list n               join cpg on n.cpg_id=cpg.id join billing_account b on cpg.billing_id=b.id join account a on cpg.account_id=a.id join product p on cpg.product_id=p.id 
#                where n.field_name='number' """
#
#    else:
#
#        sql = f"""select n.cpg_id,cpg.name,cpg.status,cpg.creation_time,cpg.sending_time,cpg.tpoa,b.company_name,a.name as account_name,p.name as product_name,cpg.xms,count(*) from cpg_blast_list 
#        n join cpg on n.cpg_id=cpg.id join billing_account b on cpg.billing_id=b.id join account a on cpg.account_id=a.id join product p on cpg.product_id=p.id 
#        where n.field_name='number' and cpg.creation_time between '{start_date}' and '{end_date}' """
#
#    if account_id:
#        sql += f"and cpg.account_id = {account_id}"
#    elif billing_id:
#        sql += f"and cpg.billing_id = {billing_id}"
#
#    sql += "group by n.cpg_id,cpg.name,cpg.status,cpg.creation_time,cpg.sending_time,cpg.tpoa,b.company_name,account_name,product_name,cpg.xms;"
#    logger.info(sql)
#
#    l_data = list()
#    data = defaultdict(dict)
#
#    cur.execute(sql)
#    rows = cur.fetchall()
#    for row in rows:
#        (cpg_id,cpg_name,status,creation_time,sending_time,tpoa,company_name,account_name,product_name,content,qty_bnumber) = row
#        creation_time = creation_time.strftime("%Y-%m-%d, %H:%M:%S")
#        try:
#            sending_time = sending_time.strftime("%Y-%m-%d, %H:%M:%S")
#        except:
#            sending_time = ""
#
#        d = {
#            "cpg_id": cpg_id,
#            "cpg_name": cpg_name,
#            "status": status,
#            "creation_time": creation_time,
#            "sending_time": sending_time,
#            "tpoa": tpoa,
#            "content": content,
#            "company_name": company_name,
#            "account_name": account_name,
#            "product_name": product_name,
#            "qty_bnumber": qty_bnumber,
#        }
#    
#        l_data.append(d)
#    
#    if len(l_data) > 0:
#        resp_json = {
#            "errorcode" : 0,
#            "status": "Success",
#            "count": len(l_data),
#            "results": l_data
#        }
#    else:
#        resp_json = {
#            "errorcode": 1,
#            "status":"No Record found!"
#        }
#        return JSONResponse(status_code=404, content=resp_json)
#    
#    return JSONResponse(status_code=200, content=resp_json)

@app.get("/iapi/internal/cpg_report") #return all campaign
async def get_all_campaign_report():
    result = func_get_campaign_report()
    return result

@app.get("/iapi/internal/cpg_report/{billing_id}") #return all campaign of this billing account
async def get_campaign_report_by_billing_id(billing_id: int):
#    sql = f"""select n.cpg_id,cpg.name,cpg.status,cpg.creation_time,cpg.sending_time,cpg.tpoa,b.company_name,a.name as account_name,p.name as product_name,cpg.xms,count(*) from cpg_blast_list n               join cpg on n.cpg_id=cpg.id join billing_account b on cpg.billing_id=b.id join account a on cpg.account_id=a.id join product p on cpg.product_id=p.id 
#                where n.field_name='number' """

#    sql += "group by n.cpg_id,cpg.name,cpg.status,cpg.creation_time,cpg.sending_time,cpg.tpoa,b.company_name,account_name,product_name,cpg.xms;"
    
    result = func_get_campaign_report(billing_id)
    return result

def func_get_campaign_report(billing_id=None):
    sql = f"""select cpg.id,cpg.name,cpg.status,cpg.creation_time,cpg.sending_time,cpg.tpoa,cpg.xms,b.company_name,a.name as account_name,p.name as product_name from cpg join billing_account b on cpg.billing_id=b.id join account a on cpg.account_id=a.id join product p on cpg.product_id=p.id """
    if billing_id:
        sql += f" where cpg.billing_id={billing_id};"
    logger.info(sql)

    l_data = list()
    data = defaultdict(dict)

    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        (cpg_id,cpg_name,cpg_status,creation_time,sending_time,tpoa,content,company_name,account_name,product_name) = row
        creation_time = creation_time.strftime("%Y-%m-%d, %H:%M:%S")
        try:
            sending_time = sending_time.strftime("%Y-%m-%d, %H:%M:%S")
        except:
            sending_time = ""

        d = {
            "cpg_id": cpg_id,
            "cpg_name": cpg_name,
            "status": cpg_status,
            "creation_time": creation_time,
            "sending_time": sending_time,
            "tpoa": tpoa,
            "content": content,
            "company_name": company_name,
            "account_name": account_name,
            "product_name": product_name
        }

        if cpg_status == "SENT": #check status, TBD: query from cdr_agg
            sql = f"""select status,sum(split),sum(selling_price) from cdr where cpg_id={cpg_id} group by status;"""
            cur.execute(sql)
            total_qty,total_cost = 0,0
            rows = cur.fetchall()
            for row in rows:
                (status,qty,cost) = row
                if not status or status == '':
                    status = 'Pending'

                d[status] = qty
                total_qty += qty
                total_cost += cost

            d["total_sent"] = total_qty
            d["cost"] = f"{total_cost:,.2f}"
    
        l_data.append(d)
    
    if len(l_data) > 0:
        resp_json = {
            "errorcode" : 0,
            "status": "Success",
            "count": len(l_data),
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No Record found!"
        }
        return JSONResponse(status_code=404, content=resp_json)
    
    logger.info("### reply client:")
    logger.info(json.dumps(resp_json, indent=4))
    
    return JSONResponse(status_code=200, content=resp_json)


def get_country_name(cid):
    cur.execute(f"select name from countries where id={cid}")
    cname = cur.fetchone()[0]
    return cname

def get_operator_name(opid):
    cur.execute(f"select name from operators where id={cid}")
    opname = cur.fetchone()[0]
    return opname

def get_countries():
    cur.execute(f"select id,name from countries")
    rows = cur.fetchall()
    d = dict()
    for row in rows:
        (id,name) = row
        d[id] = name
    return d

def get_operators():
    cur.execute(f"select id,name from operators")
    rows = cur.fetchall()
    d = dict()
    for row in rows:
        (id,name) = row
        d[id] = name
    return d

@app.get("/iapi/internal/country")#get all countries
def get_all_country():
    l_data = list()
    cur.execute(f"select id,name from countries")
    rows = cur.fetchall()
    for row in rows:
        (id,name) = row
        d = {
            "country_id": id,
            "country_name":name
        }
        l_data.append(d)

    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "count": len(l_data),
            "status": "Success",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No country found!"
        }
        return JSONResponse(status_code=404, content=resp_json)

#    logger.info("### reply internal UI:")
#    logger.info(json.dumps(resp_json, indent=4))
 
    return JSONResponse(status_code=200, content=resp_json)


@app.get("/iapi/internal/selling_price")#get all selling_price
def get_all_selling_price():
    result = func_get_selling_price()
    return result

@app.get("/iapi/internal/selling_price/{billing_id}")
def get_selling_price_by_billing_id(billing_id: int):
    result = func_get_selling_price(billing_id)
    return result


def func_get_selling_price(billing_id=None):

    d_countries = get_countries()
    d_operators = get_operators()

    sql = """select s.id,b.id as billing_id,b.company_name, a.name as account_name, p.name as product_name,s.country_id,s.operator_id,s.price,a.currency,s.validity_date 
            from selling_price s left join billing_account b on s.billing_id=b.id left join account a on s.account_id=a.id left join product p on a.product_id=p.id """

    if billing_id:
        sql += f"where s.billing_id={billing_id};"
    logger.info(sql)
    cur.execute(sql)

    l_data = list() #list of dict
    rows = cur.fetchall()
    for row in rows:
        (id,billing_id,company_name,account_name,product_name,cid,opid,price,currency,vd) = row
        cname = d_countries.get(cid)
        opname = d_operators.get(opid)
        vd = vd.strftime("%Y-%m-%d")

        d = {
            "id": id,
            "billing_id": billing_id,
            "company_name": company_name,
            "account_name": account_name,
            "product_name": product_name,
            "country_name": cname,
            "operator_name": opname,
            "price": price,
            "currency": currency,
            "validity_date": vd
        }

        l_data.append(d)
    
    resp_json = dict()

    if len(l_data) > 0:
        resp_json = {
            "errorcode":0,
            "status": "Success",
            "results": l_data
        }
    else:
        resp_json = {
            "errorcode": 1,
            "status":"No selling price found!"
        }
        return JSONResponse(status_code=404, content=resp_json)

    logger.info("### reply internal UI:")
    logger.info(json.dumps(resp_json, indent=4))
 
    return JSONResponse(status_code=200, content=resp_json)

