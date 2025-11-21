"""
DAG #2 — Build Olist Data Warehouse (STAR SCHEMA) with:
- logging framework
- alerting via on_failure_callback (Telegram)
- try/except wrapper for Python tasks
- backfill & re-fill
- parameterized run_for_date (e.g. 2017-01-01)

STAGING tables (from DAG #1):
  stg_orders
  stg_order_items
  stg_order_payments
  stg_order_reviews
  stg_customers
  stg_geolocation
"""

from datetime import datetime, timedelta
import logging
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.hooks.base import BaseHook

# ---------------------- LOGGING ----------------------

logger = logging.getLogger("ecommerce_dwh_star_schema")
logger.setLevel(logging.INFO)


# ---------------------- TELEGRAM ALERT ----------------------

def telegram_alert(context):
    """Custom Telegram alert on task failure."""
    try:
        conn = BaseHook.get_connection("telegram_conn")
        token = conn.password   # TOKEN бота
        chat_id = conn.login    # CHAT_ID

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
        logger.error(f"Telegram alert error: {e}")


# ---------------------- TRY/EXCEPT DECORATOR ----------------------

def safe_execute(func):
    """
    Обёртка, чтобы все Python-таски логировали ошибки красиво
    и не глотали исключения.
    """
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"Starting task logic: {func.__name__}")
            result = func(*args, **kwargs)
            logger.info(f"Finished task logic: {func.__name__}")
            return result
        except Exception as e:
            logger.error(f"[ERROR] in {func.__name__}: {str(e)}", exc_info=True)
            # Пробрасываем дальше, чтобы Airflow пометил таск как failed
            raise
    return wrapper


# ---------------------- DEFAULT ARGS ----------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": telegram_alert,  # включили алерт
}


with DAG(
    dag_id="ecommerce_dwh_star_schema",
    default_args=default_args,
    description="Transform Olist staging data into DWH Star Schema (dim/fact) with backfill & re-fill",
    schedule_interval="@daily",              # ежедневный запуск
    start_date=datetime(2017, 1, 1),        # примерно диапазон данных Olist
    catchup=True,                           # включаем backfill
    params={
        # если не задают run_for_date руками — берём execution date (ds)
        "run_for_date": Param(None, type=["null", "string"]),
    },
    tags=["etl", "dwh", "olist", "star-schema"],
) as dag:

    # Helper: получить дату, с которой мы работаем
    def _get_run_date(context):
        # если параметр задан явно — берём его
        run_for_date = context["params"].get("run_for_date")
        if run_for_date:
            return run_for_date
        # иначе — execution date (ds)
        return context["ds"]

    # --------------------------------------------------------
    # 1. CREATE STAR SCHEMA (DIM + FACT)
    # --------------------------------------------------------

    create_dw_schema = PostgresOperator(
        task_id="create_dw_schema",
        postgres_conn_id="postgres_etl_target_conn",
        sql="""
        -- DROP EXISTING FOR CLEAN RUN (демо-вариант)
        DROP TABLE IF EXISTS fact_order_items CASCADE;
        DROP TABLE IF EXISTS fact_order_payments CASCADE;
        DROP TABLE IF EXISTS fact_order_reviews CASCADE;
        DROP TABLE IF EXISTS dim_customer CASCADE;
        DROP TABLE IF EXISTS dim_geolocation CASCADE;
        DROP TABLE IF EXISTS dim_payment_type CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;

        -- DIM CUSTOMER
        CREATE TABLE dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_id VARCHAR UNIQUE NOT NULL,
            customer_unique_id VARCHAR,
            customer_city VARCHAR,
            customer_state VARCHAR
        );

        -- DIM GEOLOCATION
        CREATE TABLE dim_geolocation (
            geolocation_key SERIAL PRIMARY KEY,
            geolocation_zip_code_prefix INTEGER,
            geolocation_city VARCHAR,
            geolocation_state VARCHAR
        );

        -- DIM PAYMENT TYPE
        CREATE TABLE dim_payment_type (
            payment_type_key SERIAL PRIMARY KEY,
            payment_type VARCHAR UNIQUE
        );

        -- DIM DATE
        CREATE TABLE dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            is_weekend BOOLEAN
        );

        -- FACT: ORDER ITEMS
        CREATE TABLE fact_order_items (
            fact_id SERIAL PRIMARY KEY,
            order_id VARCHAR NOT NULL,
            order_item_id INTEGER NOT NULL,
            customer_key INTEGER REFERENCES dim_customer(customer_key),
            date_key INTEGER REFERENCES dim_date(date_key),
            price NUMERIC(10,2),
            freight_value NUMERIC(10,2),
            product_id VARCHAR,
            seller_id VARCHAR,
            UNIQUE(order_id, order_item_id)
        );

        -- FACT: PAYMENTS
        CREATE TABLE fact_order_payments (
            fact_id SERIAL PRIMARY KEY,
            order_id VARCHAR NOT NULL,
            customer_key INTEGER REFERENCES dim_customer(customer_key),
            payment_type_key INTEGER REFERENCES dim_payment_type(payment_type_key),
            date_key INTEGER REFERENCES dim_date(date_key),
            payment_installments INTEGER,
            payment_value NUMERIC(10,2)
        );

        -- FACT: REVIEWS
        CREATE TABLE fact_order_reviews (
            fact_id SERIAL PRIMARY KEY,
            review_id VARCHAR UNIQUE NOT NULL,
            order_id VARCHAR NOT NULL,
            customer_key INTEGER REFERENCES dim_customer(customer_key),
            date_key INTEGER REFERENCES dim_date(date_key),
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT
        );
        """,
    )

    # --------------------------------------------------------
    # 2. DIM CUSTOMER
    # --------------------------------------------------------

    @safe_execute
    def populate_dim_customer(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_customer (customer_id, customer_unique_id, customer_city, customer_state)
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state
        FROM stg_customers
        ON CONFLICT (customer_id) DO NOTHING;
        """
        hook.run(sql)
        logger.info("dim_customer populated/updated")

    populate_dim_customer_task = PythonOperator(
        task_id="populate_dim_customer",
        python_callable=populate_dim_customer,
        provide_context=True,
    )

    # --------------------------------------------------------
    # 3. DIM GEOLOCATION
    # --------------------------------------------------------

    @safe_execute
    def populate_dim_geolocation(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_geolocation (geolocation_zip_code_prefix, geolocation_city, geolocation_state)
        SELECT DISTINCT
            geolocation_zip_code_prefix,
            geolocation_city,
            geolocation_state
        FROM stg_geolocation
        WHERE geolocation_zip_code_prefix IS NOT NULL
        ON CONFLICT DO NOTHING;
        """
        hook.run(sql)
        logger.info("dim_geolocation populated/updated")

    populate_dim_geolocation_task = PythonOperator(
        task_id="populate_dim_geolocation",
        python_callable=populate_dim_geolocation,
        provide_context=True,
    )

    # --------------------------------------------------------
    # 4. DIM PAYMENT TYPE
    # --------------------------------------------------------

    @safe_execute
    def populate_dim_payment_type(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_payment_type (payment_type)
        SELECT DISTINCT payment_type
        FROM stg_order_payments
        WHERE payment_type IS NOT NULL
        ON CONFLICT (payment_type) DO NOTHING;
        """
        hook.run(sql)
        logger.info("dim_payment_type populated/updated")

    populate_dim_payment_type_task = PythonOperator(
        task_id="populate_dim_payment_type",
        python_callable=populate_dim_payment_type,
        provide_context=True,
    )

    # --------------------------------------------------------
    # 5. DIM DATE (полный диапазон)
    # --------------------------------------------------------

    @safe_execute
    def populate_dim_date(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_date (date_key, full_date, year, month, day, day_of_week, is_weekend)
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER,
            d::DATE,
            EXTRACT(YEAR FROM d),
            EXTRACT(MONTH FROM d),
            EXTRACT(DAY FROM d),
            EXTRACT(DOW FROM d),
            CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END
        FROM generate_series('2010-01-01'::DATE, '2030-12-31'::DATE, '1 day') d
        ON CONFLICT (date_key) DO NOTHING;
        """
        hook.run(sql)
        logger.info("dim_date populated/updated")

    populate_dim_date_task = PythonOperator(
        task_id="populate_dim_date",
        python_callable=populate_dim_date,
        provide_context=True,
    )

    # --------------------------------------------------------
    # 6. FACT ORDER ITEMS (re-fill per date)
    # --------------------------------------------------------

    @safe_execute
    def populate_fact_order_items(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_date = _get_run_date(context)  # 'YYYY-MM-DD'

        logger.info(f"Re-filling fact_order_items for date={run_date}")

        delete_sql = """
        DELETE FROM fact_order_items
        WHERE date_key = TO_CHAR(%(run_date)s::date, 'YYYYMMDD')::INTEGER;
        """

        insert_sql = """
        INSERT INTO fact_order_items (
            order_id, order_item_id, customer_key, date_key,
            price, freight_value, product_id, seller_id
        )
        SELECT
            oi.order_id,
            oi.order_item_id,
            dc.customer_key,
            TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
            oi.price,
            oi.freight_value,
            oi.product_id,
            oi.seller_id
        FROM stg_order_items oi
        JOIN stg_orders o ON oi.order_id = o.order_id
        JOIN dim_customer dc ON o.customer_id = dc.customer_id
        WHERE DATE(o.order_purchase_timestamp) = %(run_date)s
        ON CONFLICT (order_id, order_item_id) DO NOTHING;
        """

        hook.run(delete_sql, parameters={"run_date": run_date})
        hook.run(insert_sql, parameters={"run_date": run_date})
        logger.info(f"fact_order_items re-filled for {run_date}")

    populate_fact_order_items_task = PythonOperator(
        task_id="populate_fact_order_items",
        python_callable=populate_fact_order_items,
        provide_context=True,
    )

    # --------------------------------------------------------
    # 7. FACT PAYMENTS (re-fill per date)
    # --------------------------------------------------------

    @safe_execute
    def populate_fact_order_payments(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_date = _get_run_date(context)

        logger.info(f"Re-filling fact_order_payments for date={run_date}")

        delete_sql = """
        DELETE FROM fact_order_payments
        WHERE date_key = TO_CHAR(%(run_date)s::date, 'YYYYMMDD')::INTEGER;
        """

        insert_sql = """
        INSERT INTO fact_order_payments (
            order_id, customer_key, payment_type_key, date_key,
            payment_installments, payment_value
        )
        SELECT
            p.order_id,
            dc.customer_key,
            dpt.payment_type_key,
            TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD')::INTEGER AS date_key,
            p.payment_installments,
            p.payment_value
        FROM stg_order_payments p
        JOIN stg_orders o ON p.order_id = o.order_id
        JOIN dim_customer dc ON o.customer_id = dc.customer_id
        JOIN dim_payment_type dpt ON p.payment_type = dpt.payment_type
        WHERE DATE(o.order_purchase_timestamp) = %(run_date)s
        ON CONFLICT DO NOTHING;
        """

        hook.run(delete_sql, parameters={"run_date": run_date})
        hook.run(insert_sql, parameters={"run_date": run_date})
        logger.info(f"fact_order_payments re-filled for {run_date}")

    populate_fact_order_payments_task = PythonOperator(
        task_id="populate_fact_order_payments",
        python_callable=populate_fact_order_payments,
        provide_context=True,
    )

    # --------------------------------------------------------
    # 8. FACT REVIEWS (re-fill per date — по дате создания отзыва)
    # --------------------------------------------------------

    @safe_execute
    def populate_fact_order_reviews(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_date = _get_run_date(context)

        logger.info(f"Re-filling fact_order_reviews for date={run_date}")

        delete_sql = """
        DELETE FROM fact_order_reviews
        WHERE date_key = TO_CHAR(%(run_date)s::date, 'YYYYMMDD')::INTEGER;
        """

        insert_sql = """
        INSERT INTO fact_order_reviews (
            review_id, order_id, customer_key, date_key,
            review_score, review_comment_title, review_comment_message
        )
        SELECT
            r.review_id,
            r.order_id,
            dc.customer_key,
            TO_CHAR(r.review_creation_date::date, 'YYYYMMDD')::INTEGER AS date_key,
            r.review_score,
            r.review_comment_title,
            r.review_comment_message
        FROM stg_order_reviews r
        JOIN stg_orders o ON r.order_id = o.order_id
        JOIN dim_customer dc ON o.customer_id = dc.customer_id
        WHERE DATE(r.review_creation_date) = %(run_date)s
        ON CONFLICT (review_id) DO NOTHING;
        """

        hook.run(delete_sql, parameters={"run_date": run_date})
        hook.run(insert_sql, parameters={"run_date": run_date})
        logger.info(f"fact_order_reviews re-filled for {run_date}")

    populate_fact_order_reviews_task = PythonOperator(
        task_id="populate_fact_order_reviews",
        python_callable=populate_fact_order_reviews,
        provide_context=True,
    )

    # --------------------------------------------------------
    # DEPENDENCIES
    # --------------------------------------------------------

    create_dw_schema >> [
        populate_dim_customer_task,
        populate_dim_geolocation_task,
        populate_dim_payment_type_task,
        populate_dim_date_task,
    ]

    [
        populate_dim_customer_task,
        populate_dim_payment_type_task,
        populate_dim_date_task,
    ] >> populate_fact_order_items_task

    [
        populate_dim_customer_task,
        populate_dim_payment_type_task,
        populate_dim_date_task,
    ] >> populate_fact_order_payments_task

    [
        populate_dim_customer_task,
        populate_dim_date_task,
    ] >> populate_fact_order_reviews_task
