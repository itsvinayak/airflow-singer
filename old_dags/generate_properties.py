import os
import json

SINGER_DATA = '/root/airflow/singer_data/'
tap_stream_id="metaorigin-usercreditaccount"

#generate default properties
cmd = F"~/.virtualenvs/tap-mysql/bin/tap-mysql -c {SINGER_DATA}mysql_config.json --discover > {SINGER_DATA}properties.json"
os.system(cmd)


fpath = f"{SINGER_DATA}properties.json"

# Opening JSON file
with open(fpath) as json_file:
    data = json.load(json_file)

#initialize the schema
schema = {"streams":[]}

streams_list = data['streams']

for stream in streams_list:
    if stream["tap_stream_id"] == tap_stream_id:
        print(stream["tap_stream_id"])
        schema['streams'].append(stream)

schema['streams'][0]['metadata'][0]['metadata']['selected']= True

schema['streams'][0]['metadata'][0]['metadata']['replication-method']= "FULL_TABLE"

with open(f"{SINGER_DATA}metaorigin_properties.json", "w") as outfile: 
    json.dump(schema, outfile)