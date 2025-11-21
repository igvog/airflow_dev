from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email_smtp as send_email
import requests
import json
import logging
import pandas as pd
from pathlib import Path
import os
import traceback

DATA = "/opt/airflow/data/online_retail_II.csv"
TELEGRAM_TOKEN = "8365532997:AAG2bVd8CtodV7JKizoKRNTO8z58eIjfhhQ"
TELEGRAM_CHAT_ID = "993345413"

# ===== Logging configuration =====
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
LOG_DIR = Path(AIRFLOW_HOME) / "logs" / "project_api_to_dwh"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "project_api_to_dwh.log"

logger = logging.getLogger("project_api_to_dwh")
logger.setLevel(logging.INFO)
if not logger.handlers:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    fh = logging.FileHandler(LOG_FILE)
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
# =================================

def task_fail_alert_telegram(context):
    task = context['task_instance'].task_id
    dag = context['task_instance'].dag_id
    msg = f"❌ Task Failed\nDAG: {dag}\nTask: {task}"

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=10)
        logger.info("Sent failure alert to Telegram.")
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")
        logger.debug(traceback.format_exc())

default_args = {
    'owner': 'Balgabek',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_alert_telegram,
}

with DAG(
    'project_api_to_dwh',
    default_args=default_args,
    description='ETL pipeline from API to Postgres DWH',
    schedule_interval='@daily',
    catchup=False,
    params = {
        'run_date': '2025-11-22',
    },
) as dag:
    # ========== STAGING LAYER ==========
    def fetch_data(**context):
        try:
            df = pd.read_csv(DATA, encoding='ISO-8859-1')
            df.columns = [c.strip().replace(' ', '_') for c in df.columns]
            context['ti'].xcom_push(key='data', value=df.to_dict(orient='records'))
            logger.info(f"Fetched {len(df)} rows from CSV {DATA}")
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            logger.debug(traceback.format_exc())
            raise

    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        provide_context=True
    )

    def create_staging_table():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_etl_balga')

            drop_table_sql = "DROP TABLE IF EXISTS staging_online_retail;"
            hook.run(drop_table_sql)
            logger.info("Dropped staging table if existed.")

            create_table_sql = """
            CREATE TABLE IF NOT EXISTS staging_online_retail (
                Invoice VARCHAR,
                StockCode VARCHAR,
                Description VARCHAR,
                Quantity INT,
                InvoiceDate TIMESTAMP,
                Price FLOAT,
                CustomerID INT,
                Country VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            hook.run(create_table_sql)
            logger.info("Staging table created or already exists.")
        except Exception as e:
            logger.error(f"Error creating staging table: {e}")
            logger.debug(traceback.format_exc())
            raise

    create_staging_table_task = PythonOperator(
        task_id='create_staging_table',
        python_callable=create_staging_table
    )

    def load_data_to_staging(**context):
        try:
            data = context['ti'].xcom_pull(key='data', task_ids='fetch_data') or []
            hook = PostgresHook(postgres_conn_id='postgres_etl_balga')

            rows = []
            for row in data:
                try:
                    quantity_int = int(row.get('Quantity')) if row.get('Quantity') and not pd.isna(row.get('Quantity')) else 0
                    price_float = float(row.get('Price')) if row.get('Price') and not pd.isna(row.get('Price')) else 0.0
                    customer_id_int = None
                    if 'CustomerID' in row:
                        customer_id_int = int(row.get('CustomerID')) if row.get('CustomerID') and not pd.isna(row.get('CustomerID')) else None
                    elif 'Customer_ID' in row:
                        customer_id_int = int(row.get('Customer_ID')) if row.get('Customer_ID') and not pd.isna(row.get('Customer_ID')) else None

                    rows.append((
                        row.get('Invoice'),
                        row.get('StockCode'),
                        row.get('Description'),
                        quantity_int,
                        row.get('InvoiceDate'),
                        price_float,
                        customer_id_int,
                        row.get('Country')
                    ))
                except Exception as e:
                    logger.warning(f"Skipping row due to error: {e} | Row preview: {str(row)[:200]}")
                    logger.debug(traceback.format_exc())

            insert_sql = """
            INSERT INTO staging_online_retail (Invoice, StockCode, Description, Quantity, InvoiceDate, Price, CustomerID, Country, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING;
            """
            if rows:
                conn = hook.get_conn()
                cur = conn.cursor()
                cur.executemany(insert_sql, rows)
                conn.commit()
                cur.close()
                conn.close()
                logger.info(f"Loaded {len(rows)} rows into staging_online_retail")
            else:
                logger.info("No rows to load into staging_online_retail")
        except Exception as e:
            logger.error(f"Failed to load data into staging: {e}")
            logger.debug(traceback.format_exc())
            raise

    load_data_to_staging_task = PythonOperator(
        task_id='load_data_to_staging',
        python_callable=load_data_to_staging,
        provide_context=True
    )
    
    # ========== DATA WAREHOUSE LAYER ==========
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_balga',
        sql="""
            -- Dimensions
            DROP TABLE IF EXISTS dim_country;
            CREATE TABLE dim_country (
                country_key VARCHAR PRIMARY KEY,
                country VARCHAR
            );

            DROP TABLE IF EXISTS dim_products;
            CREATE TABLE dim_products (
                product_key VARCHAR PRIMARY KEY,
                stockcode VARCHAR,
                description VARCHAR
            );

            DROP TABLE IF EXISTS dim_customers;
            CREATE TABLE dim_customers (
                customer_key VARCHAR PRIMARY KEY,
                customerid INT,
                country VARCHAR
            );

            DROP TABLE IF EXISTS dim_date;
            CREATE TABLE dim_date (
                date_id SERIAL PRIMARY KEY,
                date DATE UNIQUE,
                year INT,
                month INT,
                day INT,
                weekday INT
            );

            -- FACT TABLE
            DROP TABLE IF EXISTS fact_sales;
            CREATE TABLE fact_sales (
                invoice_id VARCHAR,
                customer_key VARCHAR,
                product_key VARCHAR,
                date DATE,
                quantity INT,
                price FLOAT,
                line_total FLOAT,
                country_key VARCHAR
            );
        """,
    )

    # ========== LOAD DATA INTO DIM + FACT TABLES SEPARATE TASK ==========
    load_dw_task = PostgresOperator(
        task_id='load_dw',
        postgres_conn_id='postgres_etl_balga',
        sql="""
            -- ========== DIM COUNTRY ==========
            INSERT INTO dim_country (country_key, country)
            SELECT DISTINCT
                'Country_' || Country AS country_key,
                Country
            FROM staging_online_retail
            WHERE Country IS NOT NULL
            ON CONFLICT (country_key) DO NOTHING;

            -- ========== DIM PRODUCTS ==========
            INSERT INTO dim_products (product_key, stockcode, description)
            SELECT DISTINCT
                'Product_' || StockCode,
                StockCode,
                Description
            FROM staging_online_retail
            ON CONFLICT (product_key) DO NOTHING;

            -- ========== DIM CUSTOMERS ==========
            INSERT INTO dim_customers (customer_key, customerid, country)
            SELECT DISTINCT
                'Customer_' || CustomerID,
                CustomerID,
                Country
            FROM staging_online_retail
            WHERE CustomerID IS NOT NULL
            ON CONFLICT (customer_key) DO NOTHING;

            -- ========== DIM DATE ==========
            INSERT INTO dim_date (date, year, month, day, weekday)
            SELECT DISTINCT
                DATE(InvoiceDate),
                EXTRACT(YEAR FROM InvoiceDate),
                EXTRACT(MONTH FROM InvoiceDate),
                EXTRACT(DAY FROM InvoiceDate),
                EXTRACT(DOW FROM InvoiceDate)
            FROM staging_online_retail
            WHERE InvoiceDate IS NOT NULL
            ON CONFLICT (date) DO NOTHING;

            -- ========== FACT SALES ==========
            INSERT INTO fact_sales (invoice_id, customer_key, product_key, date, quantity, price, line_total, country_key)
            SELECT
                s.Invoice,
                'Customer_' || s.CustomerID,
                'Product_' || s.StockCode,
                DATE(s.InvoiceDate),
                s.Quantity,
                s.Price,
                s.Quantity * s.Price,
                'Country_' || s.Country
            FROM staging_online_retail s;
        """,
    )

    def success_notification(**context):
        try:
            msg = "✅ ETL DAG успешно завершён!"
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": msg})
            logger.info("Sent success notification to Telegram.")
        except Exception as e:
            logger.error(f"Failed to send success notification: {e}")
            logger.debug(traceback.format_exc())

    success_task = PythonOperator(
        task_id='success_notification',
        python_callable=success_notification
    )

    fetch_data_task >> create_staging_table_task >> load_data_to_staging_task >> create_dw_schema >> load_dw_task >> success_task