from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from kagglehub import dataset_download

import requests
import json
import logging


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'game_info_to_dwh_dag',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl'],
) as dag:

    drop_staging_tables = PostgresOperator(
        task_id='drop_staging_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            DROP TABLE IF EXISTS stg_games_raw;
            DROP TABLE IF EXISTS stg_games_clean;
            DROP TABLE IF EXISTS stg_game_platforms;
            DROP TABLE IF EXISTS stg_game_developers;
            DROP TABLE IF EXISTS stg_game_genres;
            DROP TABLE IF EXISTS stg_game_publishers;
        """
    )

    def download_dataset(**context) -> None:
        path = dataset_download(
            'jummyegg/rawg-game-dataset', path='game_info.csv'
        )
        context['ti'].xcom_push(key='dataset_path', value=path)
        print(path)

    download_game_dataset = PythonOperator(
        task_id='download_game_dataset',
        python_callable=download_dataset
    )

    drop_staging_tables >> download_game_dataset
