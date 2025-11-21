"""
Capstone Project: Olist E-Commerce DWH
Author: Sultan Myrzash
Description: 
    - Ingests Olist CSVs (Raw) -> Postgres Staging
    - Quality Checks (DQ)
    - Transforms -> Star Schema (Dimensions & Facts)
    - Features: Backfilling, Telegram Alerting, Auto-Translation, Date Dimension
"""

import os
import logging
import pandas as pd
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# --- CONFIGURATION ---
DATA_PATH = "/opt/airflow/data/raw" 

default_args = {
    'owner': 'sultan_myrzash',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False, 
}

# --- ALERTING FUNCTION ---
def send_telegram_alert(context):
    """Sends a failure alert to Telegram via Airflow Variables."""
    try:
        # Get vars from Airflow Admin -> Variables
        token = Variable.get("telegram_bot_token", default_var=None)
        chat_id = Variable.get("telegram_chat_id", default_var=None)
        
        if not token or not chat_id:
            logging.warning("Telegram vars not set. Skipping alert.")
            return

        task_id = context.get('task_instance').task_id
        dag_id = context.get('dag').dag_id
        log_url = context.get('task_instance').log_url
        date = context.get('ds')

        message = (
            f"🚨 **Pipeline Failed!** 🚨\n"
            f"**DAG:** {dag_id}\n"
            f"**Task:** {task_id}\n"
            f"**Data Date:** {date}\n"
            f"Check logs: {log_url}"
        )

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "Markdown"}
        requests.post(url, json=payload)
        logging.info("Telegram alert sent.")
    except Exception as e:
        logging.error(f"Failed to send alert: {e}")

import requests # Import needs to be available for the callback

@dag(
    dag_id='olist_ecommerce_dwh',
    default_args=default_args,
    description='End-to-End Olist ETL',
    schedule_interval='@daily', 
    start_date=datetime(2017, 1, 1), 
    end_date=datetime(2018, 9, 1), 
    catchup=True, # CRITICAL FOR BACKFILL DEMO
    max_active_runs=1, 
    on_failure_callback=send_telegram_alert, 
    tags=['capstone', 'olist', 'production']
)
def olist_pipeline():

    @task
    def create_schema():
        """
        DDL: Creates all Staging and DWH Tables.
        Uses IF NOT EXISTS to be idempotent.
        """
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        -- === 1. STAGING LAYER ===
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS stg_items (
            order_id TEXT,
            order_item_id INT,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TIMESTAMP,
            price NUMERIC,
            freight_value NUMERIC
        );

        CREATE TABLE IF NOT EXISTS stg_customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT,
            customer_zip_code_prefix INT,
            customer_city TEXT,
            customer_state TEXT
        );

        CREATE TABLE IF NOT EXISTS stg_products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_name_lenght INT,
            product_description_lenght INT,
            product_photos_qty INT,
            product_weight_g INT,
            product_length_cm INT,
            product_height_cm INT,
            product_width_cm INT
        );

        CREATE TABLE IF NOT EXISTS stg_category_translation (
            product_category_name TEXT,
            product_category_name_english TEXT
        );

        -- === 2. DWH LAYER (STAR SCHEMA) ===
        
        -- Dim: Products
        CREATE TABLE IF NOT EXISTS dim_products (
            product_key SERIAL PRIMARY KEY,
            product_id TEXT UNIQUE,
            category_name TEXT,
            weight_g INT,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- Dim: Customers
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_key SERIAL PRIMARY KEY,
            customer_unique_id TEXT UNIQUE,
            city TEXT,
            state TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- Dim: Date (Standard DWH Table)
        CREATE TABLE IF NOT EXISTS dim_date (
            date_key INT PRIMARY KEY,
            full_date DATE,
            year INT,
            month INT,
            day_name TEXT,
            is_weekend BOOLEAN
        );

        -- Fact: Sales
        CREATE TABLE IF NOT EXISTS fact_sales (
            fact_id SERIAL PRIMARY KEY,
            order_id TEXT,
            order_item_id INT,
            customer_key INT REFERENCES dim_customers(customer_key),
            product_key INT REFERENCES dim_products(product_key),
            date_key INT REFERENCES dim_date(date_key),
            price NUMERIC,
            freight_value NUMERIC,
            total_value NUMERIC,
            order_status TEXT,
            UNIQUE(order_id, order_item_id)
        );
        """
        hook.run(sql)
        logging.info("Schema structure verified.")

    @task
    def load_staging_csvs():
        """
        ELT: Loads raw CSVs into Staging tables.
        """
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        engine = hook.get_sqlalchemy_engine()

        files_map = {
            'olist_orders_dataset.csv': 'stg_orders',
            'olist_order_items_dataset.csv': 'stg_items',
            'olist_customers_dataset.csv': 'stg_customers',
            'olist_products_dataset.csv': 'stg_products',
            'product_category_name_translation.csv': 'stg_category_translation'
        }

        for csv_file, table_name in files_map.items():
            file_path = os.path.join(DATA_PATH, csv_file)
            
            if not os.path.exists(file_path):
                logging.warning(f"Missing file: {csv_file}")
                continue # Don't break, just log (Or raise exception if strict)

            logging.info(f"Ingesting {csv_file} -> {table_name}")
            
            try:
                df = pd.read_csv(file_path)
                # Clean specific to Olist
                if 'order_id' in df.columns:
                    df = df.dropna(subset=['order_id'])
                
                # Write to DB
                df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
                logging.info(f"Loaded {len(df)} rows.")
            except Exception as e:
                logging.error(f"Failed loading {csv_file}: {e}")
                raise AirflowFailException(f"Critical failure loading {csv_file}")

    @task
    def data_quality_check():
        """
        DQ: Checks if staging tables are not empty before proceeding.
        """
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        # Check counts
        checks = {
            'stg_orders': 0,
            'stg_items': 0,
            'stg_products': 0
        }
        
        for table, min_count in checks.items():
            records = hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            if records <= min_count:
                raise AirflowFailException(f"DQ Failed: {table} is empty!")
            logging.info(f"DQ Passed: {table} has {records} rows.")

    @task
    def transform_dimensions():
        """
        Transform: Populates Dimensions (Products, Customers, Date).
        """
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        # 1. Products (Join with English Translation)
        sql_prod = """
        INSERT INTO dim_products (product_id, category_name, weight_g)
        SELECT DISTINCT 
            p.product_id, 
            COALESCE(t.product_category_name_english, p.product_category_name, 'Unknown'),
            p.product_weight_g
        FROM stg_products p
        LEFT JOIN stg_category_translation t ON p.product_category_name = t.product_category_name
        ON CONFLICT (product_id) DO UPDATE SET 
            category_name = EXCLUDED.category_name;
        """
        
        # 2. Customers (Deduplicated by Unique ID)
        sql_cust = """
        INSERT INTO dim_customers (customer_unique_id, city, state)
        SELECT DISTINCT customer_unique_id, MAX(customer_city), MAX(customer_state)
        FROM stg_customers
        GROUP BY customer_unique_id
        ON CONFLICT (customer_unique_id) DO NOTHING;
        """

        # 3. Date Dimension (Generated)
        sql_date = """
        INSERT INTO dim_date (date_key, full_date, year, month, day_name, is_weekend)
        SELECT 
            TO_CHAR(datum, 'YYYYMMDD')::INT,
            datum,
            EXTRACT(YEAR FROM datum),
            EXTRACT(MONTH FROM datum),
            TO_CHAR(datum, 'Day'),
            CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END
        FROM generate_series('2016-01-01'::DATE, '2018-12-31'::DATE, '1 day'::INTERVAL) datum
        ON CONFLICT (date_key) DO NOTHING;
        """
        
        hook.run(sql_prod)
        hook.run(sql_cust)
        hook.run(sql_date)
        logging.info("All Dimensions transformed and loaded.")

    @task
    def transform_fact_sales(ds=None):
        """
        Loads Fact Table for the specific execution date (Backfill).
        """
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        logging.info(f"Processing Fact Sales for Date: {ds}")
        
        sql = f"""
        INSERT INTO fact_sales (
            order_id, order_item_id, customer_key, product_key, 
            date_key, price, freight_value, total_value, order_status
        )
        SELECT 
            o.order_id,
            i.order_item_id,
            dc.customer_key,
            dp.product_key,
            TO_CHAR(o.order_purchase_timestamp::TIMESTAMP, 'YYYYMMDD')::INT,
            i.price,
            i.freight_value,
            (i.price + i.freight_value) as total_value,
            o.order_status
        FROM stg_orders o
        JOIN stg_items i ON o.order_id = i.order_id
        JOIN stg_customers sc ON o.customer_id = sc.customer_id
        -- Join Dims to get Surrogate Keys
        JOIN dim_customers dc ON sc.customer_unique_id = dc.customer_unique_id
        LEFT JOIN dim_products dp ON i.product_id = dp.product_id
        WHERE 
            -- Backfill Filter
            DATE(o.order_purchase_timestamp::TIMESTAMP) = '{ds}'
        ON CONFLICT (order_id, order_item_id) DO NOTHING;
        """
        
        hook.run(sql)
        logging.info(f"Fact data loaded for {ds}.")

    # --- DAG DEPENDENCIES ---
    
    t_schema = create_schema()
    t_load = load_staging_csvs()
    t_dq = data_quality_check()
    t_dims = transform_dimensions()
    t_facts = transform_fact_sales()

    # Flow
    t_schema >> t_load >> t_dq >> t_dims >> t_facts

olist_pipeline()