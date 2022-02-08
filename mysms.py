import os
import mydb # r => redis connector, cur => postgres
import requests
import json
import re

from myutils import logger,config
from mydb import g_numbering_plan, r

notif1_expire = 4*24*3600 #redis: MSGID2:msgid2 => msgid1:::api_key:::require_dlr
sms_expire = 3*24*3600

"""
example request and response body to be displayed in API document
"""
example_create_sms={
    "normal": {
        "summary": "example with short SMS",
        "description": "Short SMS, 1-part, max 160 GSM-7bit characters or max 70 USC-2 encoded caracters",
        "value": {
            "from": "Short",
            "to": "6588001000",
            "content": "Hello World!"
        },
    },
    "concatenated": {
        "summary": "example with long SMS",
        "description": "Concatenated SMS,a long SMS segemented into mutliple part,each part is charged as separate SMS",
        "value": {
            "from": "Long",
            "to": "6588001000",
            "content": "A man being mugged by two thugs put up a tremendous fight! Finally, the thugs subdued him and took his wallet. Upon finding only two dollars in the wallet, the surprised thug said \"Why did you put up such a fight?\" To which the man promptly replied \"I was afraid that you would find the $200 hidden in my shoe!\""
        },
    },
    "bulk": {
        "summary": "bulk example: send SMS to multiple numbers",
        "description": "comma separarated MSISDN list",
        "value": {
            "from": "BulkSMS",
            "to": "6588001000,6599000100",
            "content": "Hello World!"
        },
    },
}

example_create_sms_response = {
    200: {
        "description": "success",
        "content":{
            "application/json":{
                "examples":{
                    "normal": {
                        "summary": "example with short SMS",
                        #"description": "Short SMS, 1-part, max 160 GSM-7bit characters or max 70 USC-2 encoded caracters",
                        "value": {
                            "errorcode": 0,
                            "message-count": 1,
                            "messages": [
                                {
                                    "msgid": "77b16382-7871-40bd-a1ac-a26c6ccce687",
                                    "to": "6588001000"
                                }
                            ]
                        },
                    },
                    "concatenated": {
                        "summary": "example with long SMS",
                        #"description": "Concatenated SMS,a long SMS segemented into mutliple part,each part is charged as separate SMS",
                        "value": {
                            "errorcode": 0,
                            "message-count": 3,
                            "messages": [
                                {
                                    "msgid": "77b16382-7871-40bd-a1ac-a26c6ccce687",
                                    "to": "6588001000"
                                },
                                {
                                    "msgid": "9d316085-cc29-4fb6-9522-6ad8748fcb89",
                                    "to": "6588001000"
                                },
                                {
                                    "msgid": "def6196e-3b73-4a1a-9d1b-f46cbf139645",
                                    "to": "6588001000"
                                }
                            ]
                        },
                    }, # concatenated
                    "bulk": {
                        "summary": "bulk example: send SMS to multiple numbers",
                        #"description": "Concatenated SMS,a long SMS segemented into mutliple part,each part is charged as separate SMS",
                        "value": {
                            "errorcode": 0,
                            "message-count": 2,
                            "messages": [
                                {
                                    "msgid": "77b16382-7871-40bd-a1ac-a26c6ccce687",
                                    "to": "6588001000"
                                },
                                {
                                    "msgid": "9d316085-cc29-4fb6-9522-6ad8748fcb89",
                                    "to": "6599000100"
                                }
                            ]
                        },
                    }, # concatenated

                },
            },
        },
    },
}


def create_sms_ameex(ac,data,provider): #post SMS to Ameex A2P API
    sender = data.get('sender')
    msisdn = data.get('to')
    xms = data.get('content')
    msgid1 = data.get('msgid')
    require_dlr = data.get('require_dlr')
    udh = data.get('udh')

    api_key,api_secret = config['provider_api_credential'].get(provider).split('---')
    logger.debug(f"debug provider_api_credential for {provider}: {api_key} {api_secret}")

    req_data = {
        "from": sender,
        "to": msisdn,
        "content": xms,
        'udh': udh
    }

    url = f"https://{api_key}:{api_secret}@a2p.ameex-mobile.com/api/sms"
    res = requests.post(url,json=req_data, timeout=(2,10)) #tuple:1st num means the timeout when client establish connection to the server, 2nd num means the timeout to get response(connection already established)
    res_json = res.json()
    logger.info("### Response from provider:")
    logger.debug(json.dumps(res_json,indent=4))

    res_error = int(res_json.get('errorcode',0))
    res_split = int(res_json.get('message-count',1)) #should always be 1 because I already do split, and call provider API for each part
    res_msgid = res_json.get('messages')[0].get('msgid','')
    
    if res_split != 1:
        logger.warning(f"!!! split result different from AMEEX {res_split}")

    ### TBD: redis pipeline
    ### record MSGID2:<msgid2> => <msgid1>:::<api_key>:::<require_dlr>, for callback_dlr to map msgid1 and callback_url of client
    k = f"MSGID2:{res_msgid}"
    v = f"{msgid1}:::{api_key}:::{require_dlr}"
    mydb.r.setex(k,notif1_expire, value=v)
    logger.info(f"SETEX {k} {notif1_expire} {v}")

    ### record MSGID1:<msgid1> => <msgid2>, for API endpoint /sms/:msgid1 to query_dlr, check if msgid1 exists
    k = f"MSGID1:{msgid1}"
    v = res_msgid
    mydb.r.setex(k,notif1_expire, value=v)
    logger.info(f"SETEX {k} {notif1_expire} {v}")

    return res_error,res_msgid
    
### for internal/sms to create test for newly created SMPP account
def internal_create_sms_smpp(outdir,data):
#                data = {
#                    "msgid": msgid,
#                    "sender": sender,
#                    "to": bnumber,
#                    "content": xms,
#                    "udh": udh,
#                    "dcs": dcs
#               }

        #check if account is SMPP or HTTP
    msgid = data.get("msgid")
    bnumber = data.get("to")
    error = 0
    notif1_dir = "/home/amx/notif1"
    
    subdir = re.sub(r'.*\/', '',outdir)

    """create notif1 file, empty file"""
    tmpnotif1 = f"{notif1_dir}/{subdir}/tmp-{subdir}---{bnumber}---{msgid}---"
    notif1 = f"{notif1_dir}/{subdir}/{subdir}---{bnumber}---{msgid}---"
 
    logger.info(f"create notif1 {notif1}")
    try:
        with open(tmpnotif1,'w') as w: #empty file
            pass
        os.rename(tmpnotif1,notif1)
    except IOError as e:
        logger.info(e)
        error = 7 
    except:
        logger.info(f"something bad happen,can not create {tmpoutput}")
        error = 7
    if error != 0:
        return error

    """create SMS file"""
    tmpoutput = os.path.join(outdir, f"tmp-xms{msgid}")
    output = os.path.join(outdir, f"xms{msgid}")
    try:
        with open(tmpoutput,'w', encoding='utf-8') as w:
            w.write("; encoding=UTF-8\n")
            w.write(f"[{subdir}]\n")
            w.write(f"DCS={data.get('dcs')}\n")
            w.write(f"Phone={bnumber}\n")
            w.write(f"OriginatingAddress={data.get('sender')}\n")
            w.write(f"LocalId={msgid}\n")
            w.write(f"MsgId={msgid}\n")
            w.write(f"XMS={data.get('content')}\n")
            w.write(f"StatusReportRequest=True\n") #always require DLR from our supplier

        os.rename(tmpoutput,output)
        logger.info(f"created {output}")
    except IOError as e:
        logger.info(e)
        error = 7
    except:
        logger.warning(f"something bad happen,can not create {tmpoutput}")
        error = 7
    finally:
        return error


## call HTTP API on a2p server
def create_sms(ac,data): #ac: dict inclues account info, data: dict includes sms info
#        ac = {
#        "api_key": api_key,
#        }

#            data = {
#            "msgid": msgid,
#            "sender": sender,
#            "to": msisdn,
#            "content": xms,
#            "udh": udh
#            "dcs": dcs 
#            }

    logger.info(f"### debug: account {ac}")
    logger.info(json.dumps(ac, indent=4))
    logger.info(f"### debug: sms {ac}")
    logger.info(json.dumps(data, indent=4))

    api_key = ac.get('api_key')

    error = 0
    msgid = data.get('msgid')
    sender = data.get('sender')
    bnumber = data.get('to')
    xms = data.get('content')
    udh = data.get('udh','')
    #cpg_id = data.get('cpg_id',0)
    dcs = data.get('dcs',0)

    ### - 
    d_sms = {
        "msgid": msgid,
        "tpoa": sender,
        "bnumber": bnumber,
        "xms": xms,
        "dcs": dcs
    }

    if udh:
        d_sms['udh'] = udh

    with r.pipeline() as pipe:
        ### lpush redis list HTTPIN:{api_key}: {msgid}
        redis_list = f"HTTPIN:{api_key}"
        r.lpush(redis_list, msgid)
        logger.info(f"##add msgid in list redis: LPUSH {redis_list} {msgid}")
        
        ### sms: hset redis HASH index HTTPSMS:{msgid}: 
        index = f"HTTPSMS:{msgid}"
        for k,v in d_sms.items():
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
    if msisdn in np.keys():
      result = np[msisdn]
      break
    else:
      msisdn = msisdn[:-1]  #remove last digit
  return result

    
if __name__ == '__main__':
    # msgid = str(uuid4())
    # ac = {
    #     'webuser_id': 1,
    #     'billing_id': 1,
    #     'product_id': 0
    # }

    # data = {
    #     "msgid": msgid,
    #     "sender": "NOC",
    #     "to": "+6586294138",
    #     "content": "hello world!",
    #     "country_id": 95,
    #     "operator_id": 95,
    # }
    # error,msgid2 = create_sms_ameex(ac,data,'AMEEX_PREMIUM')
    # print(f"debug call result: {error}, {msgid2}")

    result = parse_bnumber(g_numbering_plan, '+089')
    if result:
        cid,opid = result.split('---')
        print(cid,opid)

