from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/scripts')
from producer import fetch_and_send
from consumer import process_and_store

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 2, 27),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def producer_task():
    fetch_and_send()

def consumer_task():
    process_and_store()

with DAG(
    "weather_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
) as dag:
    start_producer = PythonOperator(
        task_id="start_producer",
        python_callable=producer_task,
        execution_timeout=timedelta(minutes=5),
    )

    start_consumer = PythonOperator(
        task_id="start_consumer",
        python_callable=consumer_task,
        execution_timeout=timedelta(minutes=5),
    )

    start_producer >> start_consumer