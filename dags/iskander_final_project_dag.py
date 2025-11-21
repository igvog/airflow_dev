"""
DAG - финальный проект по курсу Data Engineering
Работа с датасетом с kaggle - https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv
Этот DAG берет данные из датасета, загружает в Postgres и создает Star-схему Data Warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import requests
import json
import logging

# Дефолтные аргументы
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
    'iskander_final_project_dag',
    default_args=default_args,
    description='ETL пайплайн: Датасет → Postgres → Star Schema DW',
    schedule_interval=timedelta(hours=1),  # Ежечасно
    start_date=datetime(2025, 11, 21),
    catchup=False,
    tags=['etl', 'dataset', 'datawarehouse', 'star-schema'],
) as dag:
    def start():
        print("init")
