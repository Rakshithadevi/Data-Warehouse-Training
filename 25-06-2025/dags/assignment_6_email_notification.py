from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def task1():
    print("Task 1 completed")

def task2():
    print("Task 2 completed")

def success_email(context):
    send_email(
        to="{{ var.value.success_email_to }}",
        subject="DAG Succeeded",
        html_content="All tasks completed successfully!"
    )

with DAG("email_notification_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False,
         on_success_callback=success_email,
         default_args={
             'email': ['{{ var.value.alert_email_to }}'],
             'email_on_failure': True
         }) as dag:

    t1 = PythonOperator(
        task_id="task1",
        python_callable=task1
    )

    t2 = PythonOperator(
        task_id="task2",
        python_callable=task2
    )

    t1 >> t2
