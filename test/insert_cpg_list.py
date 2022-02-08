#!/usr/bin/env python

import random
import hashlib
import re
import sys 

def clean_msisdn(msisdn):
    number = msisdn.strip() #remove trailing whitespaces or newline
    number = re.sub(r'^\++', r'',number) #remove leading +
    number = re.sub(r'^0+', r'',number) #remove leading 0

    if re.search(r'\D+',number): #include non-digit
        return None

    number = re.sub(r'^',r'+',number) #add back leading +

    if len(number) < 11 or len(number) > 16:
        return None

    return number

def read_comma_sep_lines(l_line):

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
            return None

   
        for line in l_line[1:]: #start from 2nd line
            items = line.split(',')
            items = [i.strip() for i in items]
            print(f"clean up line: {items}")
    
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
#    lines = list()
#    lines.append("Name  , number")
#    lines.append("Max, a123456")
#    lines.append("Bob, 1234567")
#    lines.append("Alice")

    input_file = sys.argv[1]
    with open(input_file) as f:
        lines = f.readlines()

    l_data = read_comma_sep_lines(lines) #list of dict

    if l_data: #if there are valid entries from the input
        #cpg_id = random.randint(1,10000)
        cpg_id = 1
        cpg_name = "cpg" + str(cpg_id)
        tpoa = "loadtest"
        billing_id,webuser_id,product_id = 1,2,0
        xms = f"welcome to shop {cpg_id}"
        sql = f"""insert into cpg (name,tpoa,billing_id,webuser_id,product_id,xms) values ('{cpg_name}','{tpoa}',{billing_id},{webuser_id},{product_id},'{xms}');"""
        print(sql)
        
        for d in l_data:
            hash_value = d.get('hash',None)
            if hash_value:
                del d['hash'] #delete 'hash' from the dict
                for k,v in d.items():
                    sql = f"""insert into cpg_blast_list (cpg_id,field_name,value,hash) values ({cpg_id}, '{k}','{v}','{hash_value}');"""
                    print(sql)
    else:
        print("no valid entries")

"""
Examples

Valid input:
1. this should be the most common case when list only contain bnumbers, it's not compulsory to have first line to define field, we know it only contain bnumber 
number
+966502053458
+966568975942

or 
+966502053458
+966568975942

result:
--------
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (2262, 'number','+966502053458','e561fbdbb7ea9639150be091bcfcff9c');
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (2262, 'number','+966568975942','9c1e536c152bfb09519ad60e1e2b41df');


2. first line contain multiple field names, must includes 'number' field, no empty field

name,number,var1
Bob,+966502053458,hello
Alice,+966568975942,world

result:
-------
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (4122, 'name','Bob','c867aea989e21557cceb9412fe23ad0d');
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (4122, 'number','+966502053458','c867aea989e21557cceb9412fe23ad0d');
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (4122, 'var1','hello','c867aea989e21557cceb9412fe23ad0d');
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (4122, 'name','Alice','2667315f72520fe9e2659a4eb5e2898f');
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (4122, 'number','+966568975942','2667315f72520fe9e2659a4eb5e2898f');
insert into cpg_blast_list (cpg_id,field_name,value,hash) values (4122, 'var1','world','2667315f72520fe9e2659a4eb5e2898f');


Invalid input:
1. first line contain multiple fields, but there is no 'number' field defined, or there is empty field
name,var1
Bob,+966502053458,hello
Alice,+966568975942,world

or 
name,number,
Bob,+966502053458,hello
Alice,+966568975942,world



result:
--------
!!! no field name 'number' or includes field with empty value
no valid entries

"""
