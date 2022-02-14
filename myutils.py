import random
import logging
import time
import os
from configparser import ConfigParser
import hashlib
import re

#from mysms import clean_msisdn => cause circular import

basedir = os.path.abspath(os.path.dirname(__file__))
log = basedir + '/log/' + 'httpapi.log'
config_file = basedir + '/' + '.config' # to keep private info, DB password, API credential ...

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

########################
### Logging          ###
########################

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.Formatter.converter = time.gmtime
## create a console handler
#c_handler = logging.StreamHandler()
#c_handler.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler(log)
# handler.setLevel(logging.INFO)
handler.setLevel(logging.DEBUG)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handler to the logger
logger.addHandler(handler)
#logger.addHandler(c_handler)

########################
### Configurations   ###
########################

config = read_config() # read into config object

def gen_udh_base():
        rand1 = random.randint(0,15)
        rand2 = random.randint(0,15)

        udh_base = "0003" + format(rand1,'X') + format(rand2, 'X')
        return udh_base
def gen_udh(udh_base,split,i):
    return udh_base + format(split,'02d') + format(i,'02d')

def generate_otp(type,length):
# Importing string library function
    import string

    # Takes random choices from
    # ascii_letters and digits
    otp, base = '',''
    if type == 'digit':
        base = string.digits
    elif type == 'alphanumeric':
        base = string.ascii_uppercase + string.ascii_lowercase + string.digits
    elif type == 'alpha':
        base = string.ascii_uppercase + string.ascii_lowercase
    elif type == 'upper':
        base = string.ascii_uppercase + string.digits
    elif type == 'lower':
        base = string.ascii_lowercase + string.digits

    otp = ''.join( [random.choice(base) for n in range(length)] )
    return otp

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

def read_comma_sep_lines(l_line): #return -1 if format issue, return None if no valid bnumber

    ### if first line has comma, means it's headers, in this case, there must be a field named 'number', and no empty field
    headers = l_line[0].strip().split(',')
    print(f"headers: {headers}")

    d_header = dict() # hash/dict, map index => field name
    index_msisdn = 0 #by default, the 1st column is bnumber

    l_result = list() #list of dict, the dict could be {'number':'12345678'} or {'number':'12345678', 'var','some variable'}
    if len(headers) > 1: #the first line is comma separated, so it has to be the field definition
        #find index of field 'number', which contain the bnumber
        for i,name in enumerate(headers):
            # change field to lower case, strip trailng space or newline
            name = name.lower().strip()
            headers[i] = name
            d_header[i] = name

            if name == 'number':
                index_msisdn = i
                print(f"index_msisdn: {index_msisdn}")
        print(f"clean up headers: {headers}")

        if not 'number' in headers or '' in headers:
            print("!!! no field name 'number' or includes field with empty value")
            return -1 # file content format issue

        for line in l_line[1:]: #start from 2nd line
            items = line.split(',')
            items = [i.strip() for i in items]
            #print(f"clean up line: {items}")

            try:
                bnumber = clean_msisdn(items[index_msisdn])
                if bnumber: #bnumber valid
                    items[index_msisdn] = bnumber
                    d = dict()
                    for i, v in enumerate(items):
                        header = d_header[i] #get the field name
                        if v: #value not empty
                            d[header] = v
                    ## add a hash value to correlate all information for the same SMS, number,variables
                    md5 = hashlib.md5(f"{bnumber}{str(random.randint(0,10000000))}".encode()).hexdigest()
                    d['hash'] = md5
                    l_result.append(d)
            except: #no bnumber in this line
                pass

    ## first line has no comma, so this file only contains bnumber, does not care there is field definition or not
    else:
        for line in l_line:
            line = line.strip() #remove trailing space
            bnumber = clean_msisdn(line)
            if bnumber:
                md5 = hashlib.md5(f"{bnumber}{str(random.randint(0,10000000))}".encode()).hexdigest()
                l_result.append({'number':line, 'hash':md5})

    if len(l_result) == 0:
        return None
    else:
        return l_result


if __name__ == '__main__':
    udh_base = gen_udh_base()
    print(udh_base)
