import pandas as pd
import singer
import urllib.request
from datetime import datetime, timezone,date
import json,os
from airflow.models import Variable

#----------------------------variables
SINGER_DATA = Variable.get("SINGER_DATA")

#load last modified file into postgres,mysql,google-gsheet
"""
1.load schema file
2.load csv file
3.create a singer object and dump data into destination
"""
#==============================================================================================
# ===================load last modify csv file
#==============================================================================================
current_date = date.today().isoformat()  
today = date.today() 
directory=f"{SINGER_DATA}Data-"+current_date
csv_file_list=os.listdir(directory)
#get last inserted file from dir and file will be sort according time
last_file=sorted(csv_file_list)[-1]
df=pd.read_csv(os.path.join(directory,last_file))

#==============================================================================================
# ===================load schema file
#==============================================================================================
schema_file=f"{SINGER_DATA}schemas/UserCreditAccount.json"

if not os.path.exists(schema_file):
    raise  FileNotFoundError("schema file not found")

schema=json.load(open(schema_file))

singer.write_schema('user_acc', schema, 'creditid')
for index,row in df.iterrows():
	data=[{key.lower():int(row[key]) for key in row.keys()}]
	singer.write_records('user_acc', data)











