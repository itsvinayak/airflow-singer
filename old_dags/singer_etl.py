"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html

#for xcom
# https://precocityllc.com/blog/airflow-and-xcom-inter-task-communication-use-cases/
"""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# from airflow.operators.python import PythonOperator

#lets import function and run using python operator
import sys
sys.path.insert(1, '/home/navneetsajwan/airflow/singer_data')

#==================================first etl =================================
# design a buisness logic
#=============================================================================
from  mysql_to_csv import tap_mysql_target_csv,today_dir,move_files,detect_schemas,state_file


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,    
    'start_date': datetime(2020,4, 11),
    # 'end_date': datetime(2018, 12, 5),
    'email': ['rahul@metaorigins.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Set Schedule: Run pipeline once a day. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "00 21 * * *"
scriptpath = Variable.get("SINGER_DATA")
# Define DAG: Set ID and assign default args and schedule interval
dag = DAG(
    'singer_etl', 
    default_args=default_args, 
    schedule_interval=schedule_interval
    )

#===============================================================================
# extract data from mysql and store into csv file
t1= PythonOperator(
    task_id='tap_mysql_target_csv',
    python_callable=tap_mysql_target_csv,
    dag=dag,
)

#================================================================================
# create today dir if not exists
t2= PythonOperator(
    task_id='today_dir',
    python_callable=today_dir,
    dag=dag,
)

#================================================================================
# move extrated csv file into today dir folder
t3= PythonOperator(
    task_id='move_files',
    python_callable=move_files,
    dag=dag,
)

#================================================================================
# detect_schemas  from db.json file
t4= PythonOperator(
    task_id='detect_schemas',
    python_callable=detect_schemas,
    dag=dag,
)

#================================================================================
# change state.json file increamental state
t5= PythonOperator(
    task_id='state_file',
    python_callable=state_file,
    dag=dag,
)

#================================================================================
# ===============push data into target===========================================

t6 = BashOperator(
    task_id='push_into_target',
    bash_command='python3 ' + scriptpath + 'push_target.py "{{ execution_date }}"',
    retries=1,
    dag=dag)



[t1>>t2>>t3>>t4>>t5]>>t6