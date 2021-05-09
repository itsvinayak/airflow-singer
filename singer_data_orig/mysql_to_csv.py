import glob, os, os.path
import time
from datetime import date, timedelta
import zipfile
import shutil
from airflow.models import Variable
from schemaDetection import detect_schema,increamenta_state

#----------------------------variables
# SINGER_DATA = Variable.get("SINGER_DATA")
SINGER_DATA = './singer_data/'
current_date = date.today().isoformat()  
today = date.today() 
directory=f"{SINGER_DATA}Data-"+current_date

tap_stream_id='cloutcubetest-UserCreditAccount'


def tap_mysql_target_csv():
	#==========================================================================================
	#extract data from mysql
	#==========================================part-1===========================================
	# bash_cmd=f"~/.virtualenvs/tap-mysql/bin/tap-mysql -c {SINGER_DATA}mysql_config.json --properties {SINGER_DATA}db.json --state {SINGER_DATA}state.json | ~/.virtualenvs/target-csv/bin/target-csv"
	bash_cmd=f"~/.virtualenvs/tap-mysql/bin/tap-mysql -c {SINGER_DATA}mysql_config.json --properties {SINGER_DATA}metaorigin_properties.json | ~/.virtualenvs/target-csv/bin/target-csv"
	#run bash command
	os.system(bash_cmd)

def today_dir():
	#===========================================================================================
	#create folder and save csv file
	#==========================================part-2===========================================
	#get current data for creating folder
	#create directory if not exist
	if not os.path.exists(directory):
		os.makedirs(directory)


def move_files():
	#load result csv file into variable
	Transfer_data = glob.glob(os.path.join("*.csv"))
	[shutil.move(files,directory+'/'+files)for files in Transfer_data]

def detect_schemas():
	#===========================================================================================
	#detect schema
	#==========================================part-3===========================================
	#create a schema-change folder if not exist
	#first load schema and store into json file
	#filename of schema json shoud be schema_1.json
	schema_folder=f"{SINGER_DATA}schemas"
	if not os.path.exists(schema_folder):
		os.mkdir(schema_folder)

	filename=f"{SINGER_DATA}db.json"
	detect_schema(filename,tap_stream_id)

def state_file():
	#===========================================================================================
	#detect schema
	#==========================================part-4===========================================
	#update state.json file
	increamenta_state(directory,tap_stream_id)
