import glob, os, os.path
import time
from datetime import date, timedelta
import os

import zipfile
import shutil
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


import sys
import os
sys.path.insert(0, '/root/airflow/old_dags')

from schemaDetection import detect_schema



SINGER_DATA = '/root/airflow/singer_data/'
current_date = date.today().isoformat() 

directory=f"{SINGER_DATA}Data-"+current_date
tap_stream_id="metaorigin-usercreditaccount"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2021,5, 9),
    'email': ['navneet@metaorigins.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


schedule_interval = "@daily"
scriptpath = './singer_data/'
dag = DAG(
    'dag_5', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )


def tap_mysql_target_csv():
    bash_cmd=f"~/.virtualenvs/tap-mysql/bin/tap-mysql -c {SINGER_DATA}mysql_config.json --properties {SINGER_DATA}metaorigin_properties.json | ~/.virtualenvs/target-csv/bin/target-csv"
    print(os.getcwd())
    return os.system(bash_cmd)


def today_dir():
    print("create directory")
    if not os.path.exists(directory):
        os.makedirs(directory)


def move_files():
	Transfer_data = glob.glob(os.path.join("*.csv"))
	[shutil.move(files,directory+'/'+files)for files in Transfer_data]

def detect_schemas():
	schema_folder=f"{SINGER_DATA}schemas"
	if not os.path.exists(schema_folder):
		os.mkdir(schema_folder)

	filename=f"{SINGER_DATA}metaorigin_properties.json"
	detect_schema(filename,tap_stream_id)

def push_2_target():
    bash_cmd = f" ~/.virtualenvs/target-postgres/bin/python {SINGER_DATA}tap-csv.py | ~/.virtualenvs/target-postgres/bin/target-postgres --config {SINGER_DATA}target_postgres_config.json >> {SINGER_DATA}state.json"
    os.system(bash_cmd)
    print('push_test')


t0= BashOperator(
    task_id='generate_properties',
    bash_command='python /root/airflow/old_dags/generate_properties.py',
    dag=dag,
)


t1= PythonOperator(
    task_id='tap_mysql_target_csv',
    python_callable=tap_mysql_target_csv,
    dag=dag,
)

t2= PythonOperator(
    task_id='today_dir',
    python_callable=today_dir,
    dag=dag,
)

t3= PythonOperator(
    task_id='move_files',
    python_callable=move_files,
    dag=dag,
)
t4= PythonOperator(
    task_id='detect_schemas',
    python_callable=detect_schemas,
    dag=dag,
)

t5= PythonOperator(
    task_id = 'python_push_into_target',
    python_callable = push_2_target,
    dag=dag,
)


t0>>t1>>t2>>t3>>t4>>t5

