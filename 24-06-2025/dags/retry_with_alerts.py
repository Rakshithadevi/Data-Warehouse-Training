# dags/retry_with_alerts.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import random

def flaky_api_call():
    if random.random() < 0.7:
        raise Exception("API failed")
    print("API call succeeded")

def alert_final_failure(**context):
    print(f"Alert: final failure at {context['ti'].try_number}")

def success_only_task(**context):
    print("Success task executed!")

with DAG('retry_with_alerts', start_date=days_ago(1), schedule=None, catchup=False) as dag:
    api = PythonOperator(
        task_id='call_flaky_api',
        python_callable=flaky_api_call,
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        on_failure_callback=alert_final_failure
    )
    on_success = PythonOperator(
        task_id='post_success_task',
        python_callable=success_only_task,
        trigger_rule='all_success'
    )
    api >> on_success
