import glob, os, os.path,shutil,json,zipfile,time
from datetime import date, timedelta
import  pandas as pd
from airflow.models import Variable
SINGER_DATA = './singer_data/'
#store current_fileName into text file
def detect_schema(filename,tap_stream_id):
    df=open(filename)
    
    df = json.load(df)
    
    #add those column that we want to detect
    #tap_stream_id='cloutcubetest-UserCreditAccount'
    
    for table_data in df['streams']:
        if table_data['tap_stream_id']==tap_stream_id:
            table_name=table_data['table_name']
            schema_table=table_data['schema']['properties']
            schema_table =  {k.lower(): v for k, v in schema_table.items()}
            schema_json={'properties':schema_table}
            
    json_object = json.dumps(schema_json, indent = 4) 
    target_file=f"{SINGER_DATA}schemas/{table_name}.json"
    with open(target_file, 'w') as f:
        f.write(json_object)
            
    

def increamenta_state(directory,tap_stream_id):
    csv_file_list=os.listdir(directory)
    #get last inserted file from dir and file will be sort according time
    if len(csv_file_list)>0:
        last_file=sorted(csv_file_list)[-1]
        
        df=pd.read_csv(os.path.join(directory,last_file))
        
        #now we going to add this data into state.json file
        #that means again iteration will (start previous_id+current_id)
        
        #lets open file state.json
        mysql_state_file=f"{SINGER_DATA}state.json"
        state_file=json.load(open(mysql_state_file))
        
        #update value
        state_file['bookmarks'][tap_stream_id]['replication_key_value']=state_file['bookmarks'][tap_stream_id]['replication_key_value']+df.shape[0]
        
        #now save the state json file
        # target_file=f"{SINGER_DATA}state.json"
        with open(mysql_state_file,'w') as file:
            json.dump(state_file,file,indent=4)
    else:
        # raise IndexError("today no record found empty directory.")
        print("today no record found empty directory.")