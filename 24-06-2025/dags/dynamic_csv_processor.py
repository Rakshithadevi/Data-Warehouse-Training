# dags/dynamic_csv_processor.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
import pandas as pd
from pathlib import Path

@app.dag(start_date=days_ago(1), schedule=None, catchup=False)
def dynamic_csv_processor():
    @task
    def list_csvs():
        return [str(p) for p in Path('/opt/airflow/data/input').glob('*.csv')]

    @task
    def validate_and_count(file_path: str):
        df = pd.read_csv(file_path)
        if 'id' not in df.columns:
            raise ValueError(f'{file_path} missing header')
        return {'file': file_path, 'count': len(df)}

    @task
    def merge_summaries(results):
        df = pd.DataFrame(results)
        df.to_csv('/opt/airflow/data/summary_all.csv', index=False)

    files = list_csvs()
    counts = validate_and_count.expand(file_path=files)
    merge_summaries(counts)

dynamic_csv_processor_dag = dynamic_csv_processor()
