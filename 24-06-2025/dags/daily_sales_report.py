# dags/daily_sales_report.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os, shutil

def summarize_sales(**context):
    src = '/opt/airflow/data/sales.csv'
    df = pd.read_csv(src)
    summary = df.groupby('category')['amount'].sum().reset_index()
    summary.to_csv('/opt/airflow/data/sales_summary.csv', index=False)

def archive_original(**context):
    src = '/opt/airflow/data/sales.csv'
    dst = f"/opt/airflow/archive/sales_{context['ts_nodash']}.csv"
    shutil.move(src, dst)

with DAG('daily_sales_report', start_date=days_ago(1), schedule='0 6 * * *',
         catchup=False, default_args={'execution_timeout': timedelta(minutes=5)}) as dag:
    t1 = PythonOperator(task_id='summarize_sales', python_callable=summarize_sales)
    t2 = PythonOperator(task_id='archive_original', python_callable=archive_original)
    t1 >> t2
