from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sys

def validate_data():
    df = pd.read_csv('/opt/airflow/data/orders(1).csv')
    required_columns = ['order_id', 'customer_id', 'order_date', 'amount']

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    if df[required_columns].isnull().any().any():
        raise ValueError("Null values found in mandatory fields.")

def summarize_data():
    df = pd.read_csv('/opt/airflow/data/orders(1).csv')
    print("Summary:\n", df.describe())

with DAG(
    dag_id='dynamic_csv_processor',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False) as dag:

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    summarize = PythonOperator(
        task_id="summarize_data",
        python_callable=summarize_data
    )

    validate >> summarize
