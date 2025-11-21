"""
DAG #1 — Load Olist CSV files into Postgres Staging Layer
RAW → STAGING
"""

# ============================================================
# 1. ИМПОРТЫ
# ============================================================

from datetime import datetime, timedelta
import logging
import os
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models.param import Param


# ============================================================
# 2. TELEGRAM ALERT  (уже было — оставляем как есть)
# ============================================================

def telegram_alert(context):
    try:
        conn = BaseHook.get_connection("telegram_conn")
        token = conn.password
        chat_id = conn.login

        dag_id = context["dag_run"].dag_id
        task_id = context["task_instance"].task_id
        error = context.get("exception")

        text = (
            f"🚨 *Airflow Alert*\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Task:* `{task_id}`\n"
            f"*Status:* FAILED ❌\n"
            f"*Error:* `{error}`"
        )

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(
            url,
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=5,
        )
    except Exception as e:
        logging.error(f"Telegram alert error: {e}")


# ============================================================
# 3. LOGGING FRAMEWORK
# ============================================================

logger = logging.getLogger("ecommerce_raw_to_staging")
logger.setLevel(logging.INFO)


# ============================================================
# 4. TRY/EXCEPT WRAPPER
# ============================================================

def safe_execute(func):
    """Обёртка, чтобы Python tasks красиво логировались."""
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"[START] {func.__name__}")
            result = func(*args, **kwargs)
            logger.info(f"[SUCCESS] {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"[ERROR] in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper


# ============================================================
# 5. ПУТЬ К CSV
# ============================================================

DATA_DIR = "/opt/airflow/data/olist/"


# ============================================================
# 6. DEFAULT ARGS
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": telegram_alert,
}


# ============================================================
# 7. DAG
# ============================================================

with DAG(
    dag_id="ecommerce_raw_to_staging",
    default_args=default_args,
    description="Load Olist CSV files into Postgres staging",
    schedule_interval="@daily",
    start_date=datetime(2016, 10, 4),
    catchup=True,               # включена поддержка backfill
    params={
        "run_for_date": Param(None, type=["null", "string"]),
    },
    tags=["etl", "staging", "olist"],
) as dag:

    # ========================================================
    # Helper: получить run_date
    # ========================================================

    def _get_run_date(context):
        run_for_date = context["params"].get("run_for_date")
        if run_for_date:
            return run_for_date
        return context["ds"]


    # ========================================================
    # 8. CREATE STAGING TABLES
    # ========================================================

    create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="postgres_etl_target_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS stg_orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS stg_order_items (
            order_id VARCHAR,
            order_item_id INTEGER,
            product_id VARCHAR,
            seller_id VARCHAR,
            shipping_limit_date TIMESTAMP,
            price NUMERIC(10,2),
            freight_value NUMERIC(10,2),
            PRIMARY KEY (order_id, order_item_id)
        );

        CREATE TABLE IF NOT EXISTS stg_order_payments (
            order_id VARCHAR,
            payment_sequential INTEGER,
            payment_type VARCHAR,
            payment_installments INTEGER,
            payment_value NUMERIC(10,2)
        );

        CREATE TABLE IF NOT EXISTS stg_order_reviews (
            review_id VARCHAR PRIMARY KEY,
            order_id VARCHAR,
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS stg_customers (
            customer_id VARCHAR PRIMARY KEY,
            customer_unique_id VARCHAR,
            customer_zip_code_prefix INTEGER,
            customer_city VARCHAR,
            customer_state VARCHAR
        );

        CREATE TABLE IF NOT EXISTS stg_geolocation (
            geolocation_zip_code_prefix INTEGER,
            geolocation_lat NUMERIC,
            geolocation_lng NUMERIC,
            geolocation_city VARCHAR,
            geolocation_state VARCHAR
        );
        """,
    )


    # ========================================================
    # 9. UNIVERSAL CSV LOADER (with try/except)
    # ========================================================

    @safe_execute
    def load_csv_to_staging(table_name: str, csv_filename: str, **context):
        logger = logging.getLogger("airflow")

        full_path = os.path.join(DATA_DIR, csv_filename)
        logger.info(f"Loading {csv_filename} → {table_name}")

        if not os.path.exists(full_path):
            raise FileNotFoundError(full_path)

        df = pd.read_csv(full_path)
        df = df.where(pd.notnull(df), None)

        rows = df.to_records(index=False).tolist()
        columns = df.columns.tolist()
        col_list = ",".join(columns)
        placeholders = ",".join(["%s"] * len(columns))

        sql = f"""
            INSERT INTO {table_name} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING;
        """

        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        conn = hook.get_conn()
        cur = conn.cursor()

        logger.info(f"Inserting {len(rows)} rows into {table_name}")

        try:
            cur.executemany(sql, rows)
            conn.commit()
        except Exception as e:
            logger.error(f"Insert error for {table_name}: {e}")
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

        logger.info(f"Finished inserting into {table_name}")


    # ========================================================
    # 10. TASKS
    # ========================================================

    load_orders = PythonOperator(
        task_id="load_orders",
        python_callable=load_csv_to_staging,
        op_kwargs={"table_name": "stg_orders", "csv_filename": "olist_orders_dataset.csv"},
        provide_context=True,
    )

    load_order_items = PythonOperator(
        task_id="load_order_items",
        python_callable=load_csv_to_staging,
        op_kwargs={"table_name": "stg_order_items", "csv_filename": "olist_order_items_dataset.csv"},
        provide_context=True,
    )

    load_order_payments = PythonOperator(
        task_id="load_order_payments",
        python_callable=load_csv_to_staging,
        op_kwargs={"table_name": "stg_order_payments", "csv_filename": "olist_order_payments_dataset.csv"},
        provide_context=True,
    )

    load_order_reviews = PythonOperator(
        task_id="load_order_reviews",
        python_callable=load_csv_to_staging,
        op_kwargs={"table_name": "stg_order_reviews", "csv_filename": "olist_order_reviews_dataset.csv"},
        provide_context=True,
    )

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_csv_to_staging,
        op_kwargs={"table_name": "stg_customers", "csv_filename": "olist_customers_dataset.csv"},
        provide_context=True,
    )

    load_geolocation = PythonOperator(
        task_id="load_geolocation",
        python_callable=load_csv_to_staging,
        op_kwargs={"table_name": "stg_geolocation", "csv_filename": "olist_geolocation_dataset.csv"},
        provide_context=True,
    )


    # ========================================================
    # 11. DEPENDENCIES
    # ========================================================

    create_staging_tables >> [
        load_orders,
        load_order_items,
        load_order_payments,
        load_order_reviews,
        load_customers,
        load_geolocation,
    ]
