from pydantic import BaseModel, EmailStr, Field
from typing import Optional,List
from datetime import datetime

class Msg(BaseModel):
    msgid: str = Field(description="unique message ID to identify an created SMS",example="77b16382-7871-40bd-a1ac-a26c6ccce687")
    to: str = Field(description="receipient of the SMS, MSISDN, in E.164 format", 
                    min_length=10, max_length=20, example="96650403020")


class SMSResponse(BaseModel):
    errorcode: int = Field(description="indicate result of creating SMS, 0 means successful", default=0)
    message_count: int = Field(alias="message-count",description="indicate the number of SMS created (for concatenated SMS or bulk SMS)", default=1)
    messages: List[Msg]

class InternalNewCampaign(BaseModel):
    billing_id: int
    account_id: int
    blast_list: List[str]
    cpg_name: str
    cpg_tpoa: str
    cpg_xms: str
    admin_webuser_id: int
    cpg_schedule: Optional[str] # 2022-02-15, 15:47:00

class InternalSMS_BillingAccount(BaseModel):
#    billing_id: int
    account_id: int
#    product_id: int

class InternalSMS(BaseModel):
    sender: str = Field(alias='from',description="SenderID", min_length=2, max_length=11, example="Example") 
    to: str = Field(description="receipient of the SMS, MSISDN, in E.164 format", 
                    min_length=10, max_length=20, example="6588001000")
    content: str = Field(description="SMS content. it can include any unicode defined characters in UTF-8 format",
                            example="Hello World!")
    #udh: Optional[str] = Field(default="", description="for concatenated SMS, can specify udh here")

    account: InternalSMS_BillingAccount

class InternalLogin(BaseModel):
    username: str = Field(description="username",example="admin")
    password: str = Field(description="password", example="abcd")


class GetWebUser(BaseModel):
    id: int
    username: str
    #password_hash: Optional[str]
    email: Optional[str]
    bnumber: Optional[str]
    role_id: Optional[int]
    role_name: Optional[str]
    live: Optional[int]

class GetUsersResponse(BaseModel):
    errorcode: int=0
    status: str="Success"
    results: List[GetWebUser]

class GetAudit(BaseModel):
    #timestamp: datetime #error: Object of type datetime is not JSON serializable
    timestamp: str
    username: str
    auditlog: str

class GetAuditResponse(BaseModel):
    errorcode: int=0
    status: str="Success"
    results: List[GetAudit]

class MsgNotFound(BaseModel):
    errorcode: int=1
    errormsg: str = Field(default="Not found!")

class InternalInsert(BaseModel): #add all possible field here, depends on different table, some field may be null in request body
    table: str= Field(description="name of table")
    ### for billing_account
    billing_type: Optional[str] = Field(example='prepaid', description="postpaid or prepaid")
    company_name: Optional[str]
    company_address: Optional[str]
    contact_name: Optional[str]
    country: Optional[str]
    city: Optional[str]
    postal_code: Optional[str]
    billing_email: Optional[str]
    #billing_email: Optional[EmailStr] => need to handle custom validate error
    currency: Optional[str]
    contact_number: Optional[str]
    ip_list: Optional[str]

    ### for webuser
    username: Optional[str]
    ## optional field
    password_hash: Optional[str]
    email: Optional[str]
    role_id: Optional[int]
    bnumber: Optional[str]
    dashboard: Optional[int]
    cpg: Optional[int]
    pricing: Optional[int]
    sdr: Optional[int]
    ser: Optional[int]
    sdl: Optional[int]
    usermgmt: Optional[int]
    audit: Optional[int]

    ### for audit
    auditlog: Optional[str]

    ### for account, http or smpp 
    name: Optional[str]
    comment: Optional[str]
    connection_type: Optional[str]

    ## for http account
    callback_url: Optional[str]

    ## for selling_price
    account_id: Optional[int]
    country_id: Optional[int]
    operator_id: Optional[int]
    price: Optional[float]
    validity_date: Optional[str]

    ### common
    billing_id: Optional[int] #webuser, audit, whitelist_ip, smpp_account, selling_price
    webuser_id: Optional[int] #audit
    #ipaddress: Optional[str] #whitelist_ip
    product_id: Optional[int] #account
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account


class InsertBillingAccount(BaseModel):
    ## compulsory field
    company_name: str
    contact_name: str
    ## optional field
    billing_type: Optional[str] = Field(example='prepaid', description="postpaid or prepaid", default='postpaid')
    company_address: Optional[str]
    country: Optional[str]
    city: Optional[str]
    postal_code: Optional[str]
    billing_email: Optional[str] = Field(description="comma separated emails")
    currency: Optional[str]
    contact_number: Optional[str]
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account
    #billing_email: Optional[EmailStr] => need to handle custom validate error
    ip_list: Optional[str]
    
class InsertWebUser(BaseModel):
    ## compulsory field
    billing_id: int
    username: str
    role_id: int
    ## optional field
    password_hash: Optional[str]
    email: Optional[str]
    bnumber: Optional[str]
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account
    dashboard: Optional[int]
    cpg: Optional[int]
    pricing: Optional[int]
    sdr: Optional[int]
    ser: Optional[int]
    sdl: Optional[int]
    usermgmt: Optional[int]
    audit: Optional[int]

class InsertAudit(BaseModel):
    ### for audit
    billing_id: int
    webuser_id: int
    auditlog: str

#class InsertWhitelistIP(BaseModel):
#    ### for whitelist_ip
#    billing_id: int
#    ipaddress: str

class InsertSMPPAccount(BaseModel):
    billing_id: int
    name: str = Field(description="only letters and digits, no special characters, max 20 char")
    product_id: int
    connection_type: str
    comment: Optional[str] = Field(description="add note to describe this account")
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account
    ### will be generated by API
    # systemid, password, directory, notif3_dir

class InsertHTTPAccount(BaseModel):
    billing_id: int
    name: str = Field(description="only letters and digits, no special characters, max 20 char")
    product_id: int
    connection_type: str
    ## for http
    callback_url: Optional[str]
    ## optional field
    comment: Optional[str] = Field(description="add note to describe this account")
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account
    ### will be generated by API
    # api_key: str
    # api_secret: str

class InsertSellingPrice(BaseModel):
    account_id: Optional[int]
    country_id: Optional[int]
    operator_id: Optional[int]
    price: Optional[float]
    validity_date: Optional[str]
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account


example_internal_insert={
    "billing_account": {
        "summary": "insert into billing_account",
        "value": {
            "table": "billing_account",
            "billing_type": "postpaid",
            "company_name": "ABC PTE LTD",
            "company_address": "Singapore",
            "contact_name":"Bob",
            "contact_number": "+6588120901",
            "country":"Singapore",
            "city":"Singapore",
            "postal_code":"123456",
            "billing_email":"billing@abc.com,contact@abc.com",
            "currency":"USD",
            "admin_webuser_id": 1,
            "ip_list": "10.10.10.1,10.10.101.1"
        },
    },
    "webuser": {
        "summary": "insert into webuser",
        "value": {
            "table": "webuser",
            "username": "Bob",
            "billing_id": 1001,
            "role_id": 3,
            "password_hash": "somegibberishtext",
            "email": "bob@example.com",
            "bnumber": "+6511223344",
            "admin_webuser_id": 1,
            "dashboard": 1,
            "cpg": 1,
            "pricing": 1,
            "sdr": 1,
            "ser": 1,
            "sdl": 1,
            "usermgmt": 1,
            "audit": 1
        },
    },
    "audit": {
        "summary": "insert into audit",
        "value":{
            "table": "audit",
            "billing_id": 1001,
            "webuser_id": 1001,
            "auditlog": "access report"
        },
    },
#    "whitelist_ip": {
#        "summary": "insert into whitelist_ip",
#        "value":{
#            "table": "whitelist_ip",
#            "billing_id": 1001,
#            "ipaddress": "192.168.0.1"
#        },
#    },
    "smpp account": {
        "summary": "insert into account, connection_type=smpp",
        "description": "admin create smpp account for client, API return systemid/password",
        "value":{
            "table": "account",
            "billing_id": 1,
            "name": "abc",
            "product_id": 0,
            "connection_type": "smpp",
            "comment": "premium route for abc",
            "admin_webuser_id": 1
        },
    },
    "http account": {
        "summary": "insert into account, connection_type=http",
        "description": "admin create http account for client, API return api_key/secret",
        "value":{
            "table": "account",
            "billing_id": 1,
            "name": "abc",
            "product_id": 0,
            "connection_type": "http",
            "comment": "premium route for abc",
            "callback_url": "http://example.com/callback",
            "admin_webuser_id": 1
        },
    },
    "selling_price": {
        "summary": "insert into selling_price, the price CMI sell to their customers",
        "value":{
            "table": "selling_price",
            "account_id": 10003,
            "country_id": 3,
            "operator_id": 408,
            "price": 0.02,
            "validity_date": "2022-03-01",
            "admin_webuser_id": 1
        },
    },

}

class InternalDelete(BaseModel): #add all possible field here, depends on different table, some field may be null in request body
    table: str = Field(description="name of table", example="selling_price")
    id: int = Field(description="id of the record", example=10)
 
class InternalUpdate(BaseModel): #add all possible field here, depends on different table, some field may be null in request body
    table: str= Field(description="name of table")
    id: int
    ### for billing_account
    company_name: Optional[str]
    contact_name: Optional[str]
    company_address: Optional[str]
    country: Optional[str]
    city: Optional[str]
    postal_code: Optional[str]
    billing_email: Optional[str]
    billing_type: Optional[str]
    currency: Optional[str]
    contact_number: Optional[str]
    ip_list: Optional[str]

    ### for webuser
    username: Optional[str]
    password_hash: Optional[str]
    email: Optional[str]
    role_id: Optional[int]
    bnumber: Optional[str]
    dashboard: Optional[int]
    cpg: Optional[int]
    pricing: Optional[int]
    sdr: Optional[int]
    ser: Optional[int]
    sdl: Optional[int]
    usermgmt: Optional[int]
    audit: Optional[int]

    ### for whitelist_ip and smpp_account
    #ipaddress: Optional[str]
    ### for account
    name: Optional[str]
    comment: Optional[str]
    # api_key: Optional[str]
    # api_secret: Optional[str]
    product_id: Optional[int]
    callback_url: Optional[str]
    ### common field
    billing_id: Optional[int] # in table webuser, account, whitelist_ip
    deleted: Optional[int] # in table webuser, account, billing_account
    live: Optional[int] # in table webuser, account, billing_account
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin update the account

    #selling_price
    price: Optional[float]
    validity_date: Optional[str]

    # cpg campaign
    name: Optional[str]
    tpoa: Optional[str]
    xms: Optional[str]
    sending_time: Optional[str]

    
example_internal_update={
    "billing_account": {
        "summary": "update billing_account",
        "value": {
            "table": "billing_account",
            "id": 1,
            "billing_type": "postpaid",
            "company_name": "ABC PTE LTD",
            "company_address": "Singapore",
            "contact_name":"Bob",
            "contact_number":"+6512345678",
            "country":"Singapore",
            "city":"Singapore",
            "postal_code":"123456",
            "billing_email":"billing@abc.com",
            "currency":"USD",
            "admin_webuser_id": 1,
            "live": 1,
            "deleted": 0,
            "ip_list": "10.10.10.1,10.10.101.1"
        },
    },
    "webuser": {
        "summary": "update webuser",
        "value": {
            "table": "webuser",
            "id": 1,
            "username": "bob",
            "password_hash": "somegibberishtext",
            "email": "bob@example.com",
            "billing_id": 1001,
            "role_id": 3,
            "deleted": 0,
            "live": 0,
            "admin_webuser_id": 1,
            "dashboard": 1,
            "cpg": 1,
            "pricing": 1,
            "sdr": 1,
            "ser": 1,
            "sdl": 1,
            "usermgmt": 1,
            "audit": 1
        },
    },
#    "whitelist_ip": {
#        "summary": "update whitelist_ip",
#        "value": {
#            "table": "whitelist_ip",
#            "id": 1,
#            "ipaddress": "192.168.0.1",
#            "deleted": 0
#        },
#    },
    "smpp account": {
        "summary": "update account with connection_type=smpp",
        "value": {
            "table": "account",
            "id": 1,
            "name": "some other name",
            "live": 1,
            "deleted": 0,
            "comment": "some comment",
            "admin_webuser_id": 1
        },
    },
    "http account": {
        "summary": "update account with connection_type=http",
        "value": {
            "table": "account",
            "id": 1,
            "name": "some other name",
            "live": 1,
            "deleted": 0,
            "callback_url": "http://example.com/callback",
            "comment": "some comment",
            "admin_webuser_id": 1
        },
    },
    "selling_price": {
        "summary": "update price or validity_date for selling_price",
        "value": {
            "table": "selling_price",
            "id": 1,
            "price": 0.01,
            "validity_date": "2022-03-01",
            "admin_webuser_id": 1
        },
    },
    "cpg": {
        "summary": "update name/tpoa/sending_time for campaign",
        "value": {
            "table": "cpg",
            "id": 1,
            "name": "some other name",
            "tpoa": "newsender",
            "sending_time": "2022-03-01 10:00",
            "admin_webuser_id": 1
        },
    }
}

class UpdateBillingAccount(BaseModel):
    company_name: Optional[str]
    contact_name: Optional[str]
    company_address: Optional[str]
    country: Optional[str]
    city: Optional[str]
    postal_code: Optional[str]
    billing_email: Optional[str]
    billing_type: Optional[str]
    currency: Optional[str]
    contact_number: Optional[str]
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account
    live: Optional[int]
    deleted: Optional[int]
    ip_list: Optional[str]
    
# class UpdateAPICredential(BaseModel):
#     api_key: Optional[str]
#     api_secret: Optional[str]
#     webuser_id: Optional[int]
#     product_id: Optional[int]
#     billing_id: Optional[int]
#     callback_url: Optional[str]
#     friendly_name: Optional[str]
#     deleted: Optional[int]
#     live: Optional[int]
#     description: Optional[str]

class UpdateWebUser(BaseModel):
    username: Optional[str]
    password_hash: Optional[str]
    email: Optional[str]
    billing_id: Optional[int]
    role_id: Optional[int]
    bnumber: Optional[str]
    deleted: Optional[int]
    live: Optional[int]
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account
    dashboard: Optional[int]
    cpg: Optional[int]
    pricing: Optional[int]
    sdr: Optional[int]
    ser: Optional[int]
    sdl: Optional[int]
    usermgmt: Optional[int]
    audit: Optional[int]

#class UpdateWhitelistIP(BaseModel):
#    ipaddress: Optional[str]
#    deleted: Optional[int]

# class UpdateSMPPAccount(BaseModel):
#     product_id: Optional[int]
#     comment: Optional[str]

class UpdateAccount(BaseModel):
    live: Optional[int]
    deleted: Optional[int]
    comment: Optional[str]
    name: Optional[str]
    callback_url: Optional[str] #for http account only
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin add the account

class UpdateSellingPrice(BaseModel):
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin update the account
    price: Optional[float]
    validity_date: Optional[str]

class UpdateCPG(BaseModel):
    # cpg campaign
    name: Optional[str]
    tpoa: Optional[str]
    xms: Optional[str]
    sending_time: Optional[str]
    admin_webuser_id: Optional[int] #CMI admin webuser, to know which admin update the account

class InternalUpdateCpgBlastList(BaseModel):
    cpg_id: int
    blast_list: List[str]
    admin_webuser_id: Optional[int]

example_update_cpg_blast_list={
    "valid_list_with_bnumber_only": {
        "summary": "in most cases uploaded list only contain bnumber",
        "value": {
            "cpg_id":1,
            "blast_list": ["+6511223344","+6577889900"],
            "admin_webuser_id":1
        },
    },
    "valid_list_with_bnumber_and_variables": {
        "summary": "valid list with bnumber and variables",
        "value": {
            "cpg_id":1,
            "blast_list": ["name,number","Bob,+6511223344","Alice,+6577889900"],
            "admin_webuser_id":1
        },
    },
}

class PasswordHashRequest(BaseModel):
    password: str = Field(example="combination of letter,number and special characters")


example_internal_cpg={
    "valid_list_with_bnumber_only": {
        "summary": "in most cases uploaded list only contain bnumber",
        "value": {
            "billing_id":1,
            "account_id":2,
            "blast_list": ["+6511223344","+6577889900"],
            "cpg_name": "promotion for black friday",
            "cpg_tpoa": "TopShop",
            "cpg_xms": "Enjoy 50% discount",
            "admin_webuser_id":1,
            "cpg_schedule": "2022-02-15 15:47:00"
        },
    },
    "valid_list_with_bnumber_and_variables": {
        "summary": "valid list with bnumber and variables",
        "value": {
            "billing_id":1,
            "account_id":2,
            "blast_list": ["name,number","Bob,+6511223344","Alice,+6577889900"],
            "cpg_name": "promotion for black friday",
            "cpg_tpoa": "TopShop",
            "cpg_xms": "%name%, don't miss the sale, check promotion code send to %number%",
            "admin_webuser_id":1,
            "cpg_schedule": "2022-02-15 15:47:00"
        },
    },
}

class TrafficReportRequest(BaseModel):
    start_date: Optional[str] ## default past 7 days
    end_date: Optional[str]
    billing_id: Optional[int]
    account_id: Optional[int]

example_traffic_report_request = {
    "specify date range":{
        "value": {
            "start_date": "2022-01-20",
            "end_date": "2022-01-26",
            "account_id": 10003
        },
    },
    "no date range, default past 7 days":{
        "value": {
            "billing_id": 1
        },
    },
}

class TransactionRequest(BaseModel):
    start_date: Optional[str] ## default past 7 days
    end_date: Optional[str]
    billing_id: Optional[int]
    account_id: Optional[int]
    cpg_id: Optional[int]
    msgid: Optional[str]
    bnumber: Optional[str]

example_transaction_report_request = {
    "specify account_id":{
        "value": {
            "start_date": "2022-01-23",
            "end_date": "2022-01-30",
            "account_id": 10003
        },
    },
    "specify billing_id":{
        "value": {
            "billing_id": 1
        },
    },
    "specify cpg_id":{
        "value": {
            "cpg_id": 1
        },
    },
    "specify msgid":{
        "value": {
            "msgid": "351a8acd-d6f5-4129-a4e4-95d6577f81ec"
        },
    },
    "specify bnumber":{
        "value": {
            "start_date": "2022-01-23",
            "end_date": "2022-01-30",
            "bnumber": "+6544946163"
        },
    },
    "specify bnumber and billing_id (only show record for this billing_id)":{
        "value": {
            "account_id": 10003,
            "bnumber": "+6544946163"
        },
    },
    "specify bnumber and account_id (only show record for this account_id)":{
        "value": {
            "billing_id": 1,
            "bnumber": "+6544946163"
        },
    },
}

