# dags/load_to_redshift_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import subprocess

# Define default arguments for DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'load_to_redshift',
    default_args=default_args,
    description='Load historical data from S3 to Redshift daily',
    schedule_interval=timedelta(days=1),  
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def run_load_to_redshift():
    """Function to execute load_to_redshift.py using subprocess"""
    script_path = '/path/to/src/load_to_redshift.py'
    subprocess.run(['python', script_path], check=True)

# Define the task
load_to_redshift_task = PythonOperator(
    task_id='load_data_to_redshift',
    python_callable=run_load_to_redshift,
    dag=dag,
)

load_to_redshift_task
