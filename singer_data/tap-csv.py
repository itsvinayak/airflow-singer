import singer
import urllib.request
from datetime import datetime, timezone
import pandas as pd
from datetime import date, timedelta
import os
current_date = date.today().isoformat()  
today = date.today()
SINGER_DATA = '/home/navneetsajwan/airflow/singer_data/'
directory=f"{SINGER_DATA}Data-"+current_date
# directory="/home/navneetsajwan/airflow/singer_data/temp"

# importing the module
import json
  
# Opening JSON file
with open('/home/navneetsajwan/airflow/singer_data/schemas/usercreditaccount.json') as json_file:
    data = json.load(json_file)


schema={
   	 "properties": {
   		 "creditid": {
   			 "inclusion": "automatic",
   			 "minimum": -2147483648,
   			 "maximum": 2147483647,
   			 "type": [
   				 "null",
   				 "integer"
   			 ]
   		 },
   		 "userid": {
   			 "inclusion": "available",
   			 "minimum": -2147483648,
   			 "maximum": 2147483647,
   			 "type": [
   				 "null",
   				 "integer"
   			 ]
   		 },
   		 "totalcreditscore": {
   			 "inclusion": "available",
   			 "minimum": -2147483648,
   			 "maximum": 2147483647,
   			 "type": [
   				 "null",
   				 "integer"
   			 ]
   		 }
   	 }
}

csv_file_list=os.listdir(directory)
last_file=sorted(csv_file_list)[-1]
df=pd.read_csv(os.path.join(directory,last_file))
# print(df)
singer.write_schema('user_acc', data, 'creditid')
# for index,row in df.iterrows():
#     singer.write_records('user_acc', [{'creditid':int(row['creditid']),'userid':int(row['userid']),'totalcreditscore':int(row['totalcreditscore'])}])

for index,row in df.iterrows():
    singer.write_records('user_acc',
	 [{'creditid':int(row['creditid']),
	 'userid':int(row['userid']),
	 'totalcreditscore':int(row['totalcreditscore']),
	 'creditamount':int(row['creditamount'])}])
