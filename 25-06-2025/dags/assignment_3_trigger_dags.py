from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

with DAG("parent_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    start = EmptyOperator(task_id="start")

    trigger = TriggerDagRunOperator(
        task_id="trigger_child",
        trigger_dag_id="child_dag",
        conf={"triggered_date": "{{ ds }}"}
    )

    start >> trigger
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def receive_metadata(**kwargs):
    print(f"Triggered by parent. Received metadata: {kwargs['dag_run'].conf}")

with DAG("child_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:

    receive = PythonOperator(
        task_id="receive_metadata",
        python_callable=receive_metadata,
        provide_context=True
    )
