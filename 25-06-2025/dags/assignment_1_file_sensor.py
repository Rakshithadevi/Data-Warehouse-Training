from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import shutil

def process_file():
    with open('/opt/airflow/data/incoming/report.csv', 'r') as f:
        print(f.read())

def move_to_archive():
    shutil.move('/opt/airflow/data/incoming/report.csv', '/opt/airflow/data/archive/report.csv')

with DAG("file_sensor_pipeline",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="data/incoming/report.csv",
        fs_conn_id="fs_default",
        timeout=600,
        poke_interval=30,
        mode='poke'
    )

    process = PythonOperator(
        task_id="process_file",
        python_callable=process_file
    )

    archive = PythonOperator(
        task_id="move_to_archive",
        python_callable=move_to_archive
    )

    wait_for_file >> process >> archive
