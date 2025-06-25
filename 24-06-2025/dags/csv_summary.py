from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import csv

def file_exists_check(**context):
    path = os.path.join('/opt/airflow/data', 'customers.csv')
    if not os.path.isfile(path):
        raise FileNotFoundError(f"{path} not found")

def count_rows(**context):
    path = os.path.join('/opt/airflow/data', 'customers.csv')
    with open(path) as f:
        reader = csv.reader(f)
        count = sum(1 for _ in reader) - 1  # discount header
    context['ti'].xcom_push(key='row_count', value=count)

def log_count(**context):
    count = context['ti'].xcom_pull(key='row_count')
    print(f"Total customer rows: {count}")

def alert_message():
    echo = "echo 'Count > 100: Action needed!'"
    return echo

with DAG('csv_summary_dag', start_date=days_ago(1), schedule_interval=None, catchup=False) as dag:
    t1 = PythonOperator(task_id='check_file', python_callable=file_exists_check)
    t2 = PythonOperator(task_id='count_rows', python_callable=count_rows)
    t3 = PythonOperator(task_id='log_count', python_callable=log_count)
    t4 = BashOperator(
        task_id='alert_if_big',
        bash_command=alert_message(),
        trigger_rule='all_done'  # ← keep only one
    )

    t1 >> t2 >> t3 >> t4
