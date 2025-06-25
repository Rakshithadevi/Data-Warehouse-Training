from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time

def long_task():
    print("Starting long task...")
    time.sleep(20)
    print("Completed.")

with DAG(
    dag_id='dynamic_csv_processor',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
)as dag:

    run_task = PythonOperator(
        task_id="simulate_long_work",
        python_callable=long_task,
        retries=3,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(seconds=15)
    )
