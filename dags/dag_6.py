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
sys.path.insert(0, '/home/navneetsajwan/airflow/old_dags')

from schemaDetection import detect_schema



SINGER_DATA = '/home/navneetsajwan/airflow/singer_data/'
current_date = date.today().isoformat() 

directory=f"{SINGER_DATA}Data-"+current_date
tap_stream_id="metaorigin-usercreditaccount"

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2021,5, 7),
    'email': ['navneet@metaorigins.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


schedule_interval = "@daily"
scriptpath = './singer_data/'
dag = DAG(
    'dag_6', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

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


t4>>t5