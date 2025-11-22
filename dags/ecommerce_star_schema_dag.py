"""
FINAL PROJECT: eCommerce Data Warehouse
Architecture: ELT with Daily Partitioning
Capacity: Tested on 1M rows/day batch (scalable to TBs)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# === ВАЖНО: НАСТРОЙКИ ===
DB_CONN_ID = 'postgres_etl_target_conn'
# Твоя дата старта (где лежит твой миллион строк)
START_DATE = datetime(2019, 10, 1) 

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0, # Для демо можно без ретраев, чтобы быстрее упало если что
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=60), 
}

with DAG(
    'ecommerce_final_project', 
    default_args=default_args,
    description='Production ELT Pipeline for eCommerce',
    schedule_interval='@daily', # Разбиваем по дням
    start_date=START_DATE,
    catchup=True,           # Важно: Чтобы он обработал именно 2019 год
    max_active_runs=1,      # Очередь: один день за раз
    tags=['final-project', 'elt'],
) as dag:

    # 1. Инициализация схемы (DDL)
    init_schema = PostgresOperator(
        task_id='init_schema',
        postgres_conn_id=DB_CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS dim_users (
            user_key SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dim_products (
            product_key SERIAL PRIMARY KEY,
            product_id INTEGER UNIQUE NOT NULL,
            category_code VARCHAR(255),
            brand VARCHAR(255)
        );
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_key DATE PRIMARY KEY,
            year INT, month INT, day INT, is_weekend BOOLEAN
        );
        CREATE TABLE IF NOT EXISTS fact_events (
            event_id BIGSERIAL PRIMARY KEY,
            event_time TIMESTAMP,
            user_key INTEGER REFERENCES dim_users(user_key),
            product_key INTEGER REFERENCES dim_products(product_key),
            date_key DATE REFERENCES dim_dates(date_key),
            event_type VARCHAR(50),
            price NUMERIC(10, 2),
            user_session VARCHAR(100)
        );
        -- Индексы для скорости (Обязательно!)
        CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_events(date_key);
        """
    )

    # 2. Users (Только те, кто был в этот день)
    load_users = PostgresOperator(
        task_id='load_users',
        postgres_conn_id=DB_CONN_ID,
        sql="""
        INSERT INTO dim_users (user_id)
        SELECT DISTINCT user_id
        FROM raw_ecommerce_events
        WHERE event_time >= '{{ ds }}'::TIMESTAMP 
          AND event_time < '{{ next_ds }}'::TIMESTAMP
          AND user_id IS NOT NULL
        ON CONFLICT (user_id) DO NOTHING;
        """
    )

    # 3. Products (Только товары этого дня)
    load_products = PostgresOperator(
        task_id='load_products',
        postgres_conn_id=DB_CONN_ID,
        sql="""
        INSERT INTO dim_products (product_id, category_code, brand)
        SELECT DISTINCT ON (product_id) 
            product_id, category_code, brand
        FROM raw_ecommerce_events
        WHERE event_time >= '{{ ds }}'::TIMESTAMP 
          AND event_time < '{{ next_ds }}'::TIMESTAMP
          AND product_id IS NOT NULL
        ORDER BY product_id, event_time DESC
        ON CONFLICT (product_id) DO UPDATE 
        SET category_code = EXCLUDED.category_code, brand = EXCLUDED.brand;
        """
    )

    # 4. Dates (Календарь)
    load_dates = PostgresOperator(
        task_id='load_dates',
        postgres_conn_id=DB_CONN_ID,
        sql="""
        INSERT INTO dim_dates (date_key, year, month, day, is_weekend)
        SELECT 
            '{{ ds }}'::DATE,
            EXTRACT(YEAR FROM '{{ ds }}'::DATE),
            EXTRACT(MONTH FROM '{{ ds }}'::DATE),
            EXTRACT(DAY FROM '{{ ds }}'::DATE),
            CASE WHEN EXTRACT(DOW FROM '{{ ds }}'::DATE) IN (0, 6) THEN TRUE ELSE FALSE END
        ON CONFLICT (date_key) DO NOTHING;
        """
    )

    # 5. Facts (Миллион строк за один INSERT)
    load_facts = PostgresOperator(
        task_id='load_facts',
        postgres_conn_id=DB_CONN_ID,
        sql="""
        -- Очистка (чтобы можно было перезапускать)
        DELETE FROM fact_events WHERE date_key = '{{ ds }}'::DATE;

        -- Заливка
        INSERT INTO fact_events (
            event_time, user_key, product_key, date_key, 
            event_type, price, user_session
        )
        SELECT 
            raw.event_time,
            u.user_key,
            p.product_key,
            d.date_key,
            raw.event_type,
            raw.price,
            raw.user_session
        FROM raw_ecommerce_events raw
        INNER JOIN dim_users u ON raw.user_id = u.user_id
        INNER JOIN dim_products p ON raw.product_id = p.product_id
        INNER JOIN dim_dates d ON raw.event_time::DATE = d.date_key
        WHERE 
            raw.event_time >= '{{ ds }}'::TIMESTAMP 
            AND raw.event_time < '{{ next_ds }}'::TIMESTAMP;
        """
    )

    init_schema >> [load_users, load_products, load_dates] >> load_facts