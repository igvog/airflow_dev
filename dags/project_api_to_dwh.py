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

DATA_1 = "/opt/airflow/data/online_retail_II.csv"
DATA_2 = "/opt/airflow/data/OECD_HEALTH_STAT_CANCER_STATISTICS.csv"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

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
    def fetch_data_1(**context):
        try:
            df = pd.read_csv(DATA_1, encoding='ISO-8859-1')

            df.columns = [c.strip().replace(' ', '_') for c in df.columns]

            context['ti'].xcom_push(key='data', value=df.to_dict(orient='records'))

            logger.info(f"Fetched {len(df)} rows from CSV {DATA_1}")

        except Exception as e:

            logger.error(f"Failed to read CSV: {e}")

            logger.debug(traceback.format_exc())

            raise


    def fetch_data_2(**context):
        try:
            df = pd.read_csv(DATA_2, encoding='ISO-8859-1')

            # Normalize column names to snake_case lower
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

            # Ensure flag-related columns are normalized to exactly 'flag_codes' and 'flags'
            # map variants like 'flagcodes', 'flag_codes', 'flag_codes ', 'flag code', etc.
            col_map = {}
            for col in df.columns:
                normalized = col.replace("_", "")
                if "flag" in normalized and "code" in normalized and "flag_codes" not in df.columns:
                    col_map[col] = "flag_codes"
                elif col == "flags" and "flags" not in df.columns:
                    col_map[col] = "flags"
            if col_map:
                df = df.rename(columns=col_map)

            # If either column is still missing, add it as None so downstream code can rely on their presence
            if "flag_codes" not in df.columns:
                df["flag_codes"] = None
            if "flags" not in df.columns:
                df["flags"] = None

            logger.info(f"OECD columns after normalization: {df.columns.tolist()}")
            logger.info(f"Fetched {len(df)} OECD rows")

            # Push under key 'data' so downstream tasks that pull key='data' find it
            context["ti"].xcom_push(key="oecd_data", value=df.to_dict(orient="records"))
        except Exception as e:
            logger.error(f"Failed to read OECD CSV: {e}")
            logger.debug(traceback.format_exc())
            raise

    fetch_online_retail_data_task = PythonOperator(

            task_id='fetch_online_retail_data',

            python_callable=fetch_data_1,

            provide_context=True

        )


    fetch_oecd_health_data_task = PythonOperator(

            task_id='fetch_Oecd_health_data',
            python_callable=fetch_data_2,

            provide_context=True

        )

    def create_staging_tables():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_etl_balga')

            drop_table_sql = """
            DROP TABLE IF EXISTS staging_online_retail;
            DROP TABLE IF EXISTS staging_oecd_health;
            """
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

            CREATE TABLE IF NOT EXISTS staging_oecd_health (
                var VARCHAR,
                variable VARCHAR,
                unit VARCHAR,
                measure VARCHAR,
                cou VARCHAR,
                country VARCHAR,
                yea VARCHAR,
                year INT,
                value FLOAT,
                flag_codes VARCHAR,
                flags VARCHAR,
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
        python_callable=create_staging_tables
    )

    def load_data_to_staging_online_retail(**context):
        try:
            data = context['ti'].xcom_pull(
                key='data',
                task_ids='fetch_online_retail_data'
            ) or []

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

    load_data_to_staging_online_retail_task = PythonOperator(
        task_id='load_data_to_staging_online_retail',
        python_callable=load_data_to_staging_online_retail,
        provide_context=True
    )

    def populate_staging_oecd_health(**context):
        try:
            data = context['ti'].xcom_pull(
                key='oecd_data',
                task_ids='fetch_Oecd_health_data'
            ) or []

            logger.info(f"Pulled OECD rows from XCOM: {len(data)}")

            hook = PostgresHook(postgres_conn_id='postgres_etl_balga')

            rows = []
            for row in data:
                try:
                    year_int = int(row.get('year')) if row.get('year') not in [None, "", " "] else None
                    value_float = float(row.get('value')) if row.get('value') not in [None, "", " "] else None

                    rows.append((
                        row.get('var'),
                        row.get('variable'),
                        row.get('unit'),
                        row.get('measure'),
                        row.get('cou'),
                        row.get('country'),
                        row.get('yea'),
                        year_int,
                        value_float,
                        row.get('flag_codes'),
                        row.get('flags'),
                    ))
                except Exception as e:
                    logger.warning(f"Skipping row: {e}")
                    logger.debug(traceback.format_exc())

            insert_sql = """
            INSERT INTO staging_oecd_health 
            (var, variable, unit, measure, cou, country, yea, year, value, flag_codes, flags, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT DO NOTHING;
            """

            if rows:
                conn = hook.get_conn()
                cur = conn.cursor()
                cur.executemany(insert_sql, rows)
                conn.commit()
                cur.close()
                conn.close()
                logger.info(f"Loaded {len(rows)} rows into staging_oecd_health")
            else:
                logger.info("No rows to load into staging_oecd_health")

        except Exception as e:
            logger.error(f"Failed to load OECD staging: {e}")
            logger.debug(traceback.format_exc())
            raise

    load_data_to_staging_oecd_health_task = PythonOperator(
        task_id='load_data_to_staging_oecd_health',
        python_callable=populate_staging_oecd_health,
        provide_context=True
    )

    def normalize_oecd():
        try:
            hook = PostgresHook(postgres_conn_id='postgres_etl_balga')

            normalize_sql = """
                ALTER TABLE staging_oecd_health DROP COLUMN IF EXISTS var;
                ALTER TABLE staging_oecd_health DROP COLUMN IF EXISTS flag_codes;
                ALTER TABLE staging_oecd_health DROP COLUMN IF EXISTS flags;

            """
            hook.run(normalize_sql)
            logger.info("Normalized data in staging_oecd_health.")
        except Exception as e:
            logger.error(f"Error normalizing OECD data: {e}")
            logger.debug(traceback.format_exc())
            raise

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

            --Dimension OECD
            DROP TABLE IF EXISTS dim_country_oecd cascade;
            CREATE TABLE dim_country_oecd (
                country_key VARCHAR PRIMARY KEY,
                country VARCHAR
            );
            
            DROP TABLE IF EXISTS dim_indicator cascade;
            CREATE TABLE IF NOT EXISTS dim_indicator (
                indicator_key SERIAL PRIMARY KEY,
                var VARCHAR,
                variable VARCHAR,
                unit VARCHAR,
                measure VARCHAR
            );

            -- FACT TABLE
            DROP TABLE IF EXISTS fact_oecd_health;
            CREATE TABLE IF NOT EXISTS fact_oecd_health (
                country_key VARCHAR REFERENCES dim_country_oecd(country_key),
                indicator_key INT REFERENCES dim_indicator(indicator_key),
                yea VARCHAR,
                year INT,
                value FLOAT,
                flag_codes VARCHAR,
                flags VARCHAR,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );


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
            -- ========== DIM COUNTRY OECD ==========
            INSERT INTO dim_country_oecd (country_key, country)
            SELECT DISTINCT
                'Country_' || country AS country_key,
                country
            FROM staging_oecd_health
            WHERE country IS NOT NULL
            ON CONFLICT (country_key) DO NOTHING;

            -- ========== DIM INDICATOR ==========
            INSERT INTO dim_indicator (variable, unit, measure)
            SELECT DISTINCT
                variable,
                unit,
                measure
            FROM staging_oecd_health
            WHERE variable IS NOT NULL
            ON CONFLICT (indicator_key) DO NOTHING;

            -- ========== FACT OECD HEALTH ==========
            INSERT INTO fact_oecd_health (country_key, indicator_key, yea, year, value, loaded_at)
            SELECT
                'Country_' || s.country AS country_key,
                d.indicator_key,
                s.yea,
                s.year,
                s.value,
                CURRENT_TIMESTAMP
            FROM staging_oecd_health s
            JOIN dim_indicator d
                ON s.variable = d.variable
                AND s.unit = d.unit
                AND s.measure = d.measure
            WHERE s.value IS NOT NULL;

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

    def merge_staging_data(**context):
        try:
            hook = PostgresHook(postgres_conn_id='postgres_etl_balga')
            conn = hook.get_conn()
            cur = conn.cursor()

            # Создаём таблицу для объединённых данных, если ещё нет
            cur.execute("""
                CREATE TABLE IF NOT EXISTS staging_merged (
                    invoice VARCHAR,
                    stockcode VARCHAR,
                    description VARCHAR,
                    quantity INT,
                    invoicedate TIMESTAMP,
                    price FLOAT,
                    customerid INT,
                    country VARCHAR,
                    variable VARCHAR,
                    unit VARCHAR,
                    measure VARCHAR,
                    yea VARCHAR,
                    year INT,
                    value FLOAT,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()

            # Удаляем старые данные перед новой загрузкой
            cur.execute("TRUNCATE TABLE staging_merged;")
            conn.commit()

            # Выполняем объединение и вставку
            insert_sql = """
                INSERT INTO staging_merged (
                    invoice, stockcode, description, quantity, invoicedate, price, customerid, country,
                    variable, unit, measure, yea, year, value, loaded_at
                )
                SELECT
                    r.invoice, r.stockcode, r.description, r.quantity, r.invoicedate, r.price, r.customerid, r.country,
                    o.variable, o.unit, o.measure, o.yea, o.year, o.value, CURRENT_TIMESTAMP
                FROM staging_online_retail r
                LEFT JOIN staging_oecd_health o
                    ON r.country = o.country;
            """
            cur.execute(insert_sql)
            conn.commit()

            cur.close()
            conn.close()

            logger.info("Merged data successfully loaded into staging_merged")

        except Exception as e:
            logger.error(f"Error merging staging data: {e}")
            logger.debug(traceback.format_exc())
            raise

    merge_staging_task = PythonOperator(
        task_id='merge_staging_data',
        python_callable=merge_staging_data,
        provide_context=True
    )


    normalize_oecd_task = PythonOperator(
        task_id='normalize_oecd', 
        python_callable=normalize_oecd
    )  

    [fetch_oecd_health_data_task, fetch_online_retail_data_task] >> create_staging_table_task  
    create_staging_table_task >> [load_data_to_staging_online_retail_task, load_data_to_staging_oecd_health_task]
    [load_data_to_staging_online_retail_task, load_data_to_staging_oecd_health_task] >> normalize_oecd_task 
    normalize_oecd_task >> create_dw_schema >> load_dw_task >> merge_staging_task >>success_task
    