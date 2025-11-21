from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import os

POSTGRES_CONN_ID = 'postgres_dwh'
DAG_ID = '03_build_staging_layer'

# Вычисляем абсолютный путь к папке с SQL файлами
# __file__ - это путь к текущему файлу DAG
# Мы идем в папку sql/staging, которая лежит рядом
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(DAG_FOLDER, 'sql', 'staging')

default_args = {
    'owner': 'bekzat',
    'start_date': days_ago(1),
    'retries': 1,
}

def read_sql_file(filename):
    """Читает SQL файл напрямую, минуя Jinja template search"""
    file_path = os.path.join(SQL_PATH, filename)
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except FileNotFoundError:
        raise Exception(f"❌ Файл не найден по пути: {file_path}. Запустите create_sql_files.py!")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Raw -> Staging transformation',
    schedule_interval=None,
    catchup=False,
    tags=['modeling', 'staging'],
) as dag:

    # 1. Создаем схему
    create_schema = PostgresOperator(
        task_id='create_staging_schema',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS staging;"
    )

    # 2. Staging Orders
    stg_orders = PostgresOperator(
        task_id='build_stg_orders',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=read_sql_file('stg_orders.sql') # Читаем файл напрямую
    )

    # 3. Staging Items
    stg_items = PostgresOperator(
        task_id='build_stg_items',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=read_sql_file('stg_items.sql')
    )

    # 4. Staging Products
    stg_products = PostgresOperator(
        task_id='build_stg_products',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=read_sql_file('stg_products.sql')
    )

    create_schema >> [stg_orders, stg_items, stg_products]