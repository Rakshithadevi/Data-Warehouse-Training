from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime as dt

def check_time():
    now = dt.datetime.now()
    if now.weekday() >= 5:
        return 'skip_dag'
    elif now.hour < 12:
        return 'morning_task'
    else:
        return 'afternoon_task'

with DAG("time_based_tasks",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    check = BranchPythonOperator(
        task_id="check_time",
        python_callable=check_time
    )

    skip = EmptyOperator(task_id="skip_dag")
    morning = EmptyOperator(task_id="morning_task")
    afternoon = EmptyOperator(task_id="afternoon_task")
    cleanup = EmptyOperator(task_id="cleanup", trigger_rule="none_failed_min_one_success")

    check >> [morning, afternoon, skip]
    morning >> cleanup
    afternoon >> cleanup
