
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

from  mysql_to_csv import tap_mysql_target_csv,today_dir,move_files,detect_schemas,state_file

SINGER_DATA = './singer_data/'

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2021,5, 6),
    'email': ['navneet@metaorigins.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
import subprocess

def push_2_target():
    # os.system(cmd)
    bashCommand= f"tap-mysql -c {SINGER_DATA}mysql_config.json --properties {SINGER_DATA}metaorigin_properties.json | target-csv"
    # return os.system(bash_cmd)
    # path = os.getcwd()
    # bashCommand = "cwm --rdf test.rdf --ntriples > test.nt"

    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    print("out!", output)
    print("error!", error)
    # os.system(bash_cmd)
    # print("path....",os.getcwd())
    # return path


schedule_interval = "@daily"
scriptpath = './singer_data/'

dag = DAG(
    'sql_to_csv', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )


t1= PythonOperator(
    task_id='push2target',
    python_callable=push_2_target,
    dag=dag,
)

t2 = BashOperator(
    task_id='location_bash',
    bash_command='pwd',
    retries=1,
    dag=dag)

t1>>t2
