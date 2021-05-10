import os
import json
#generate default properties
SINGER_DATA = '/home/navneetsajwan/airflow/singer_data/'
cmd = F"~/.virtualenvs/tap-mysql/bin/tap-mysql -c {SINGER_DATA}mysql_config.json --discover > {SINGER_DATA}properties.json"
os.system(cmd)


fpath = f"{SINGER_DATA}properties.json"
# Opening JSON file
with open(fpath) as json_file:
    data = json.load(json_file)



def detect_schema(filename,tap_stream_id):
    df=open(filename)
    
    df = json.load(df)
    
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