"""
API -> Minio
"""

import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json

ROCKETS_URL = "https://api.spacexdata.com/v4/rockets"
LAUNCHPADS_URL = "https://api.spacexdata.com/v4/launchpads"
LAUNCHES_URL = "https://api.spacexdata.com/v4/launches"
PAYLOADS_URL = "https://api.spacexdata.com/v4/payloads"
BUCKET_NAME = "storage"

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def fetch_and_upload_rockets(**context):
    # 1. Получаем данные с API
    try:
        
        response = requests.get(ROCKETS_URL)
        response.raise_for_status()
        rockets_data = response.json()
        logging.info(f"Fetched {len(rockets_data)} posts from API")
        
        response = requests.get(LAUNCHPADS_URL)
        response.raise_for_status()
        launchpads_data = response.json()
        logging.info(f"Fetched {len(launchpads_data)} posts from API")
        
        response = requests.get(LAUNCHES_URL)
        response.raise_for_status()
        launches_data = response.json()
        logging.info(f"Fetched {len(launches_data)} posts from API")
        
        response = requests.get(PAYLOADS_URL)
        response.raise_for_status()
        payloads_data = response.json()
        logging.info(f"Fetched {len(payloads_data)} posts from API")
        
    except Exception as e:
        logging.error(f"Error fetching API data: {str(e)}")
        raise
    
    # 2. 
    json_rockets_data = json.dumps(rockets_data, indent=4)
    json_launchpads_data = json.dumps(launchpads_data, indent=4)
    json_launches_data = json.dumps(launches_data, indent=4)
    json_payloads_data = json.dumps(payloads_data, indent=4)
    
    json_dict = {
        "raw_rockets.json": json_rockets_data, 
        "raw_launchpads.json": json_launchpads_data, 
        "raw_launches.json": json_launches_data, 
        "raw_payloads.json": json_payloads_data
        }
    
    # 3. Загружаем в MinIO через S3Hook
    s3 = S3Hook(aws_conn_id="minio_conn")
    for key, string in json_dict.items():
        s3.load_string(
            string_data=string,
            key=key,
            bucket_name=BUCKET_NAME,
            replace=True  # перезаписываем при повторном запуске
        )
        logging.info(f"Загружено {len(string)} символов в {BUCKET_NAME}/{key}")

# --- DAG ---
with DAG(
    dag_id="raw_from_api_to_minio",
    default_args=default_args,
    start_date=datetime(2025, 11, 21),
    schedule_interval=None,  # запускаем вручную
    catchup=False,
    tags=["spacex", "raw", "api", "minio"]
) as dag:

    load_rockets_task = PythonOperator(
        task_id="fetch_and_upload_rockets",
        python_callable=fetch_and_upload_rockets
    )

    load_rockets_task