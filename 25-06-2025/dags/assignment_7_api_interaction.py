from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import json

def parse_response(response_text):
    data = json.loads(response_text)
    print("Parsed API Response:", data)

with DAG("external_api_dag",
         start_date=datetime(2023, 1, 1),
         schedule_interval="@daily",
         catchup=False) as dag:

    query_api = SimpleHttpOperator(
        task_id='get_crypto_price',
        http_conn_id='coin_api',
        endpoint='v1/exchangerate/BTC/USD',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        do_xcom_push=True
    )

    parse = PythonOperator(
        task_id="parse_api_response",
        python_callable=lambda ti: parse_response(ti.xcom_pull(task_ids="get_crypto_price")),
    )

    query_api >> parse
