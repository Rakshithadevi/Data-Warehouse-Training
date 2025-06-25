# dags/branch_by_size.py
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

def choose_path():
    path = '/opt/airflow/data/inventory.csv'
    size = os.path.getsize(path) / 1024  # KB
    return 'light_summary' if size < 500 else 'detailed_processing'

with DAG('branch_by_size', start_date=days_ago(1), schedule=None, catchup=False) as dag:
    b = BranchPythonOperator(task_id='branch', python_callable=choose_path)
    light = BashOperator(task_id='light_summary', bash_command="echo 'Light summary done'")
    detailed = BashOperator(task_id='detailed_processing', bash_command="echo 'Detailed processing done'")
    cleanup = BashOperator(task_id='cleanup', bash_command="echo 'Cleanup done'", trigger_rule='none_failed_min_one_success')
    b >> [light, detailed] >> cleanup
