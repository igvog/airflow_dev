from __future__ import annotations

import logging
from datetime import timedelta

import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago


DWH_SCHEMA = "dwh_ecommerce"
STAGING_SCHEMA = "staging"
POSTGRES_CONN_ID = "postgres_etl_target_conn"
DATA_PATH = "/usr/local/airflow/data/" 


OLIST_FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

logger = logging.getLogger("airflow.task")


def load_csv_to_postgres_staging(conn_id: str, staging_schema: str, table_name: str, file_name: str):
    """
    Загружает CSV-файл из Docker Volume Mount в указанную staging-таблицу PostgreSQL.
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    
    full_path = DATA_PATH + file_name
    logger.info(f"Начало загрузки файла {full_path} в таблицу {staging_schema}.{table_name}")

    try:
        df = pd.read_csv(full_path, low_memory=False)
        
        df.to_sql(
            table_name,
            con=hook.get_sqlalchemy_engine(),
            schema=staging_schema,
            if_exists='replace',
            index=False,
            chunksize=10000
        )
        logger.info(f"Успешно загружено {len(df)} строк в {staging_schema}.{table_name}")
    except FileNotFoundError as e:
        logger.error(f"Файл не найден. Убедитесь, что он находится в вашей локальной папке ./data, и что Docker Volume Mount работает: {e}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise

SQL_CREATE_SCHEMAS = f"""
CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};
CREATE SCHEMA IF NOT EXISTS {DWH_SCHEMA};
"""

SQL_CREATE_DIMENSIONS = f"""
-- 1. dim_date
CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    month_of_year INT,
    day_of_week TEXT
);

-- 2. dim_customer
CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_unique_id TEXT NOT NULL UNIQUE,
    customer_city TEXT,
    customer_state CHAR(2)
);

-- 3. dim_product
CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE,
    product_category_name TEXT,
    product_category_name_english TEXT
);

-- 4. dim_seller
CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.dim_seller (
    seller_key SERIAL PRIMARY KEY,
    seller_id TEXT NOT NULL UNIQUE,
    seller_city TEXT,
    seller_state CHAR(2)
);
"""

SQL_CREATE_FACT = f"""
CREATE TABLE IF NOT EXISTS {DWH_SCHEMA}.fact_order_items (
    order_item_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES {DWH_SCHEMA}.dim_date (date_key),
    customer_key INT REFERENCES {DWH_SCHEMA}.dim_customer (customer_key),
    product_key INT REFERENCES {DWH_SCHEMA}.dim_product (product_key),
    seller_key INT REFERENCES {DWH_SCHEMA}.dim_seller (seller_key),
    
    order_id TEXT NOT NULL,
    order_item_id INT NOT NULL,

    price DECIMAL(10, 2) NOT NULL,
    freight_value DECIMAL(10, 2) NOT NULL,
    
    total_item_revenue DECIMAL(10, 2) GENERATED ALWAYS AS (price + freight_value) STORED,
    
    purchase_date DATE NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON {DWH_SCHEMA}.fact_order_items (date_key);
"""

SQL_POPULATE_DIM_CUSTOMER = f"""
INSERT INTO {DWH_SCHEMA}.dim_customer (customer_unique_id, customer_city, customer_state)
SELECT 
    t_new.customer_unique_id, 
    t_new.customer_city, 
    t_new.customer_state
FROM (
    SELECT DISTINCT ON (t1.customer_unique_id)
        t1.customer_unique_id, 
        t1.customer_city, 
        t1.customer_state
    FROM {STAGING_SCHEMA}.customers t1
) t_new
LEFT JOIN {DWH_SCHEMA}.dim_customer t_existing 
    ON t_new.customer_unique_id = t_existing.customer_unique_id
WHERE t_existing.customer_unique_id IS NULL;
"""

SQL_POPULATE_DIM_PRODUCT = f"""
INSERT INTO {DWH_SCHEMA}.dim_product (product_id, product_category_name, product_category_name_english)
SELECT 
    t_new.product_id, 
    t_new.product_category_name,
    t_new.product_category_name_english
FROM (
    SELECT DISTINCT ON (t1.product_id)
        t1.product_id, 
        t1.product_category_name,
        t2.product_category_name_english
    FROM {STAGING_SCHEMA}.products t1
    LEFT JOIN {STAGING_SCHEMA}.category_translation t2 
        ON t1.product_category_name = t2.product_category_name
) t_new
LEFT JOIN {DWH_SCHEMA}.dim_product t_existing ON t_new.product_id = t_existing.product_id
WHERE t_existing.product_id IS NULL;
"""

SQL_POPULATE_DIM_SELLER = f"""
INSERT INTO {DWH_SCHEMA}.dim_seller (seller_id, seller_city, seller_state)
SELECT DISTINCT 
    t1.seller_id, 
    t1.seller_city, 
    t1.seller_state
FROM {STAGING_SCHEMA}.sellers t1
LEFT JOIN {DWH_SCHEMA}.dim_seller t2 ON t1.seller_id = t2.seller_id
WHERE t2.seller_id IS NULL;
"""

SQL_POPULATE_DIM_DATE = f"""
INSERT INTO {DWH_SCHEMA}.dim_date (date_key, full_date, year, month_of_year, day_of_week)
SELECT DISTINCT
    CAST(REPLACE(TRIM(SUBSTRING(t1.order_purchase_timestamp, 1, 10)), '-', '') AS INT) AS date_key,
    TRIM(SUBSTRING(t1.order_purchase_timestamp, 1, 10))::date AS full_date,
    EXTRACT(YEAR FROM TRIM(SUBSTRING(t1.order_purchase_timestamp, 1, 10))::date) AS year,
    EXTRACT(MONTH FROM TRIM(SUBSTRING(t1.order_purchase_timestamp, 1, 10))::date) AS month_of_year,
    TO_CHAR(TRIM(SUBSTRING(t1.order_purchase_timestamp, 1, 10))::date, 'Day') AS day_of_week
FROM {STAGING_SCHEMA}.orders t1
LEFT JOIN {DWH_SCHEMA}.dim_date t2 
    ON TRIM(SUBSTRING(t1.order_purchase_timestamp, 1, 10))::date = t2.full_date
WHERE t1.order_purchase_timestamp IS NOT NULL
  AND t2.full_date IS NULL;
"""

SQL_POPULATE_FACTS = f"""
-- Шаг 1: Удаляем данные за текущую дату выполнения DAG
DELETE FROM {DWH_SCHEMA}.fact_order_items WHERE purchase_date = '{{{{ ds }}}}';

-- Шаг 2: Вставка данных, присоединяя суррогатные ключи
INSERT INTO {DWH_SCHEMA}.fact_order_items (
    date_key,
    customer_key,
    product_key,
    seller_key,
    order_id,
    order_item_id,
    price,
    freight_value,
    purchase_date
)
SELECT
    CAST(REPLACE(TRIM(SUBSTRING(t2.order_purchase_timestamp, 1, 10)), '-', '') AS INT) AS date_key,
    t4.customer_key,
    t5.product_key,
    t6.seller_key,
    t1.order_id,
    t1.order_item_id,
    t1.price,
    t1.freight_value,
    TRIM(SUBSTRING(t2.order_purchase_timestamp, 1, 10))::date AS purchase_date
    
FROM {STAGING_SCHEMA}.order_items t1
INNER JOIN {STAGING_SCHEMA}.orders t2 ON t1.order_id = t2.order_id
INNER JOIN {STAGING_SCHEMA}.customers t3 ON t2.customer_id = t3.customer_id
INNER JOIN {DWH_SCHEMA}.dim_customer t4 ON t3.customer_unique_id = t4.customer_unique_id
INNER JOIN {DWH_SCHEMA}.dim_product t5 ON t1.product_id = t5.product_id
INNER JOIN {DWH_SCHEMA}.dim_seller t6 ON t1.seller_id = t6.seller_id

"""


from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

with DAG(
    dag_id="olist_ecommerce_dwh_etl",
    start_date=datetime(2016, 9, 4),
    schedule_interval='@daily',
    catchup=True, 
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["dwh", "olist", "star_schema"],
) as dag:
    
    start_task = EmptyOperator(task_id="start_pipeline")

    create_schemas = PostgresOperator(
        task_id="create_staging_and_dwh_schemas",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_CREATE_SCHEMAS,
    )

    create_dimensions_ddl = PostgresOperator(
        task_id="create_dimension_tables_ddl",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_CREATE_DIMENSIONS,
    )

    create_fact_ddl = PostgresOperator(
        task_id="create_fact_table_ddl",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_CREATE_FACT,
    )

    load_staging_tasks = []
    for table_name, file_name in OLIST_FILES.items():
        task = PythonOperator(
            task_id=f"load_{table_name}_to_staging",
            python_callable=load_csv_to_postgres_staging,
            op_kwargs={
                "conn_id": POSTGRES_CONN_ID,
                "staging_schema": STAGING_SCHEMA,
                "table_name": table_name,
                "file_name": file_name,
            },
        )
        load_staging_tasks.append(task)

    populate_dim_customer = PostgresOperator(
        task_id="populate_dim_customer",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_POPULATE_DIM_CUSTOMER,
    )

    populate_dim_product = PostgresOperator(
        task_id="populate_dim_product",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_POPULATE_DIM_PRODUCT,
    )
    
    populate_dim_seller = PostgresOperator(
        task_id="populate_dim_seller",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_POPULATE_DIM_SELLER,
    )
    
    populate_dim_date = PostgresOperator(
        task_id="populate_dim_date",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_POPULATE_DIM_DATE,
    )

    populate_fact_table = PostgresOperator(
        task_id="populate_fact_order_items",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SQL_POPULATE_FACTS,
        params={"execution_date": "{{ ds }}"}
    )

    end_task = EmptyOperator(task_id="end_pipeline")

    start_task >> create_schemas >> load_staging_tasks

    for task in load_staging_tasks:
        task.set_downstream(create_dimensions_ddl)
        task.set_downstream(create_fact_ddl)

    create_dimensions_ddl >> [
        populate_dim_customer, 
        populate_dim_product, 
        populate_dim_seller,
        populate_dim_date
    ]

    [
        create_fact_ddl, 
        populate_dim_customer, 
        populate_dim_product, 
        populate_dim_seller,
        populate_dim_date
    ] >> populate_fact_table

    populate_fact_table >> end_task