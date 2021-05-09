import singer
import urllib.request
from datetime import datetime, timezone
import pandas as pd
from datetime import date, timedelta
import os
current_date = date.today().isoformat()  
today = date.today()
# directory="Data-"+current_date
directory="temp"

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

singer.write_schema('user_acc', schema, 'creditid')
for index,row in df.iterrows():
    # print(row['CreditId'],row['UserId'],row['TotalCreditScore'])
    # singer.write_records('user_acc', [{'creditid':int(row['CreditId']),'userid':int(row['UserId']),'totalcreditscore':int(row['TotalCreditScore'])}])
    singer.write_records('user_acc', [{'creditid':int(row['creditid']),'userid':int(row['userid']),'totalcreditscore':int(row['totalcreditscore'])}])
