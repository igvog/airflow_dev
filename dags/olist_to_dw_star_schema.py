"""
ETL DAG: Olist Brazilian E-Commerce -> Postgres DW (Star Schema)

What it does (high level):
- Ensures Olist CSV файлы доступны локально (можно принудительно перекачать).
- Создаёт staging таблицы, bulk COPY из CSV.
- Создаёт витрину (dim_customers, dim_sellers, dim_products, dim_dates, fact_order_items, fact_payments).
- Наполняет справочники и факты с привязкой к логической дате run_date.

Параметры DAG:
- run_date: логическая дата загрузки (используется как load_date в фактах и для date_key), по умолчанию 2024-01-01.
- full_refresh: True дропает и создаёт staging/dim/fact перед загрузкой; False оставляет структуру и делает MERGE/UPSERT.
- refresh_data_files: True удаляет локальные CSV и перекачивает их заново из репозитория.

Таблицы:
- Staging: staging_orders/customers/order_items/order_payments/products/sellers/geolocation/product_category_translation.
- Dimensions: dim_customers, dim_sellers, dim_products (с переводом категорий), dim_dates (2015-2030).
- Facts: fact_order_items (позиции заказа с датами purchase/approved/delivery/estimate/shipping_limit) и fact_payments (платежи по заказам).

Безопасность/наблюдаемость:
- Логирование через logging + email-алерты на сбой (_alert_on_failure).
- Параметризованный backfill: можно триггерить DAG с любым run_date, не ломая остальные даты.
"""

import logging
import os
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pendulum
import requests
from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

logger = logging.getLogger("olist_to_dw_star_schema")
logger.setLevel(logging.INFO)
logging.basicConfig(
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    level=logging.INFO,
)

ALERT_EMAILS: List[str] = [
    email.strip()
    for email in os.getenv("ALERT_EMAILS", "admin@example.com").split(",")
    if email.strip()
]
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DATA_BASE_URL = "https://raw.githubusercontent.com/olist/work-at-olist-data/master/datasets"
DATA_DIR = Path(os.getenv("OLIST_DATA_DIR", "/opt/airflow/data/olist"))

STAGING_FILES: Dict[str, Dict[str, List[str]]] = {
    "staging_orders": {
        "filename": "olist_orders_dataset.csv",
        "columns": [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    },
    "staging_customers": {
        "filename": "olist_customers_dataset.csv",
        "columns": [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    },
    "staging_order_items": {
        "filename": "olist_order_items_dataset.csv",
        "columns": [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ],
    },
    "staging_order_payments": {
        "filename": "olist_order_payments_dataset.csv",
        "columns": [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ],
    },
    "staging_products": {
        "filename": "olist_products_dataset.csv",
        "columns": [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
    },
    "staging_sellers": {
        "filename": "olist_sellers_dataset.csv",
        "columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
    },
    "staging_geolocation": {
        "filename": "olist_geolocation_dataset.csv",
        "columns": [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state",
        ],
    },
    "staging_product_category_translation": {
        "filename": "product_category_name_translation.csv",
        "columns": [
            "product_category_name",
            "product_category_name_english",
        ],
    },
}


def _alert_on_failure(context):
    """Send a short email/Telegram notification if any task fails."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context.get("task_instance").log_url

    subject = f"[Airflow][{dag_id}] Task failed: {task_id}"
    html_content = f"""
    <h3>Task failure in {dag_id}</h3>
    <ul>
      <li><b>Task:</b> {task_id}</li>
      <li><b>Run:</b> {run_id}</li>
      <li><b>Log:</b> <a href="{log_url}">{log_url}</a></li>
    </ul>
    """

    try:
        send_email(to=ALERT_EMAILS, subject=subject, html_content=html_content)
    except Exception as exc:  # pragma: no cover - alert must not break DAG
        logger.error("Failed to send failure alert: %s", exc)
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": f"[Airflow][{dag_id}] Task failed: {task_id} (run_id={run_id})\\nLog: {log_url}",
            }
            resp = requests.post(url, json=payload, timeout=15)
            resp.raise_for_status()
        except Exception as exc:  # pragma: no cover - alert must not break DAG
            logger.error("Failed to send Telegram alert: %s", exc)
    else:
        logger.info("Telegram not configured; skipping Telegram alert")


def _resolve_run_date(context) -> pendulum.Date:
    """Return the logical run date used for inserts and backfills."""
    param_date: Optional[str] = context["params"].get("run_date")
    if param_date:
        parsed = pendulum.parse(param_date).date()
        logger.info("Using parameterized run_date=%s", parsed)
        return parsed
    execution_date = context["data_interval_end"].date()
    logger.info("Using data interval end as run_date=%s", execution_date)
    return execution_date


def _download_if_missing():
    """Download Olist CSV files to DATA_DIR if they are missing."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    for table, cfg in STAGING_FILES.items():
        filename = cfg["filename"]
        target = DATA_DIR / filename
        if target.exists():
            logger.info("File exists, skipping download: %s (%s bytes)", target, target.stat().st_size)
            continue
        url = f"{DATA_BASE_URL}/{filename}"
        try:
            logger.info("Downloading %s -> %s", url, target)
            with requests.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(target, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
        except Exception:
            logger.exception("Failed to download %s", url)
            raise
        logger.info("Downloaded %s (%s bytes)", target, target.stat().st_size)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _alert_on_failure,
}


with DAG(
    "olist_to_dw_star_schema",
    default_args=default_args,
    description="ETL pipeline: Olist CSV -> Postgres -> Star Schema DW",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    tags=["etl", "olist", "datawarehouse", "star-schema"],
    params={
        "run_date": Param(
            default="2024-01-01",
            type="string",
            description="Logical load date, e.g. 2024-01-01 (used for load_date fields).",
        ),
        "full_refresh": Param(
            default=True,
            type="boolean",
            description="Drop/recreate staging and DW tables before load.",
        ),
        "refresh_data_files": Param(
            default=False,
            type="boolean",
            description="Force re-download of source CSV files even if present.",
        ),
        "force_fail": Param(
            default=False,
            type="boolean",
            description="Raise an exception early to test alerting.",
        ),
    },
) as dag:

    def ensure_data_files(**context):
        """Ensure Olist CSVs are present locally; optionally re-download."""
        refresh_data_files: bool = bool(context["params"].get("refresh_data_files"))
        if refresh_data_files:
            logger.info("Forcing data refresh; deleting existing files in %s", DATA_DIR)
            for p in DATA_DIR.glob("*.csv"):
                p.unlink()
        _download_if_missing()

    ensure_data = PythonOperator(
        task_id="ensure_data_files",
        python_callable=ensure_data_files,
    )

    def maybe_fail(**context):
        """Optional fail-fast hook to validate alerting."""
        if bool(context["params"].get("force_fail")):
            raise RuntimeError("force_fail=true — intentional failure for alert test")

    fail_switch = PythonOperator(
        task_id="maybe_fail",
        python_callable=maybe_fail,
    )

    def create_staging_tables(**context):
        """Create staging tables (drop if full_refresh)."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        full_refresh: bool = bool(context["params"].get("full_refresh"))

        drop_sql = """
        DROP TABLE IF EXISTS staging_orders CASCADE;
        DROP TABLE IF EXISTS staging_customers CASCADE;
        DROP TABLE IF EXISTS staging_order_items CASCADE;
        DROP TABLE IF EXISTS staging_order_payments CASCADE;
        DROP TABLE IF EXISTS staging_products CASCADE;
        DROP TABLE IF EXISTS staging_sellers CASCADE;
        DROP TABLE IF EXISTS staging_geolocation CASCADE;
        DROP TABLE IF EXISTS staging_product_category_translation CASCADE;
        """

        create_sql = """
        CREATE TABLE IF NOT EXISTS staging_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS staging_customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT,
            customer_zip_code_prefix INTEGER,
            customer_city TEXT,
            customer_state TEXT
        );

        CREATE TABLE IF NOT EXISTS staging_order_items (
            order_id TEXT,
            order_item_id INTEGER,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TIMESTAMP,
            price NUMERIC,
            freight_value NUMERIC,
            PRIMARY KEY (order_id, order_item_id)
        );

        CREATE TABLE IF NOT EXISTS staging_order_payments (
            order_id TEXT,
            payment_sequential INTEGER,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value NUMERIC,
            PRIMARY KEY (order_id, payment_sequential)
        );

        CREATE TABLE IF NOT EXISTS staging_products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_name_lenght NUMERIC,
            product_description_lenght NUMERIC,
            product_photos_qty NUMERIC,
            product_weight_g NUMERIC,
            product_length_cm NUMERIC,
            product_height_cm NUMERIC,
            product_width_cm NUMERIC
        );

        CREATE TABLE IF NOT EXISTS staging_sellers (
            seller_id TEXT PRIMARY KEY,
            seller_zip_code_prefix INTEGER,
            seller_city TEXT,
            seller_state TEXT
        );

        CREATE TABLE IF NOT EXISTS staging_geolocation (
            geolocation_zip_code_prefix INTEGER,
            geolocation_lat NUMERIC,
            geolocation_lng NUMERIC,
            geolocation_city TEXT,
            geolocation_state TEXT
        );

        CREATE TABLE IF NOT EXISTS staging_product_category_translation (
            product_category_name TEXT PRIMARY KEY,
            product_category_name_english TEXT
        );
        """

        if full_refresh:
            hook.run(drop_sql)
        hook.run(create_sql)
        if not full_refresh:
            hook.run("TRUNCATE TABLE staging_orders, staging_customers, staging_order_items, staging_order_payments, staging_products, staging_sellers, staging_geolocation, staging_product_category_translation;")
        logger.info("Staging tables ready (full_refresh=%s)", full_refresh)

    create_staging = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables,
    )

    def load_table(table_name: str, filename: str, columns: List[str]):
        """Return a callable that loads a CSV into a staging table via COPY."""

        def _loader(**_context):
            path = DATA_DIR / filename
            if not path.exists():
                raise FileNotFoundError(f"Missing source file: {path}")
            hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
            copy_sql = f"COPY {table_name} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER DELIMITER ',' NULL ''"
            try:
                hook.copy_expert(copy_sql, filename=str(path))
            except Exception:
                logger.exception("Failed to load %s from %s", table_name, path)
                raise
            logger.info("Loaded %s from %s", table_name, path)

        return _loader

    load_tasks = []
    for table_name, cfg in STAGING_FILES.items():
        task = PythonOperator(
            task_id=f"load_{table_name}",
            python_callable=load_table(table_name, cfg["filename"], cfg["columns"]),
        )
        load_tasks.append(task)

    def create_dw_schema(**context):
        """Create DW tables (drop if full_refresh)."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        full_refresh: bool = bool(context["params"].get("full_refresh"))

        drop_sql = """
        DROP TABLE IF EXISTS fact_payments CASCADE;
        DROP TABLE IF EXISTS fact_order_items CASCADE;
        DROP TABLE IF EXISTS dim_products CASCADE;
        DROP TABLE IF EXISTS dim_sellers CASCADE;
        DROP TABLE IF EXISTS dim_customers CASCADE;
        DROP TABLE IF EXISTS dim_dates CASCADE;
        """

        create_sql = """
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_key SERIAL PRIMARY KEY,
            customer_id TEXT UNIQUE NOT NULL,
            customer_unique_id TEXT,
            zip_code_prefix INTEGER,
            city TEXT,
            state TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_sellers (
            seller_key SERIAL PRIMARY KEY,
            seller_id TEXT UNIQUE NOT NULL,
            zip_code_prefix INTEGER,
            city TEXT,
            state TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_products (
            product_key SERIAL PRIMARY KEY,
            product_id TEXT UNIQUE NOT NULL,
            product_category_name TEXT,
            product_category_name_english TEXT,
            product_weight_g NUMERIC,
            product_length_cm NUMERIC,
            product_height_cm NUMERIC,
            product_width_cm NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS dim_dates (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name VARCHAR(20),
            day INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            is_weekend BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS fact_order_items (
            order_item_key SERIAL PRIMARY KEY,
            order_id TEXT NOT NULL,
            order_item_id INTEGER NOT NULL,
            customer_key INTEGER REFERENCES dim_customers(customer_key),
            seller_key INTEGER REFERENCES dim_sellers(seller_key),
            product_key INTEGER REFERENCES dim_products(product_key),
            order_status TEXT,
            order_purchase_date_key INTEGER REFERENCES dim_dates(date_key),
            order_approved_date_key INTEGER REFERENCES dim_dates(date_key),
            delivered_carrier_date_key INTEGER REFERENCES dim_dates(date_key),
            delivered_customer_date_key INTEGER REFERENCES dim_dates(date_key),
            estimated_delivery_date_key INTEGER REFERENCES dim_dates(date_key),
            shipping_limit_date_key INTEGER REFERENCES dim_dates(date_key),
            price NUMERIC,
            freight_value NUMERIC,
            load_date DATE DEFAULT CURRENT_DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(order_id, order_item_id)
        );

        CREATE TABLE IF NOT EXISTS fact_payments (
            order_payment_key SERIAL PRIMARY KEY,
            order_id TEXT NOT NULL,
            payment_sequential INTEGER NOT NULL,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value NUMERIC,
            customer_key INTEGER REFERENCES dim_customers(customer_key),
            order_purchase_date_key INTEGER REFERENCES dim_dates(date_key),
            load_date DATE DEFAULT CURRENT_DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(order_id, payment_sequential)
        );

        CREATE INDEX IF NOT EXISTS idx_fact_order_items_customer_key ON fact_order_items(customer_key);
        CREATE INDEX IF NOT EXISTS idx_fact_order_items_seller_key ON fact_order_items(seller_key);
        CREATE INDEX IF NOT EXISTS idx_fact_order_items_product_key ON fact_order_items(product_key);
        CREATE INDEX IF NOT EXISTS idx_fact_payments_order_id ON fact_payments(order_id);
        """

        if full_refresh:
            hook.run(drop_sql)
        hook.run(create_sql)
        logger.info("DW schema ready (full_refresh=%s)", full_refresh)

    create_dw = PythonOperator(
        task_id="create_dw_schema",
        python_callable=create_dw_schema,
    )

    def populate_dim_customers(**_context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_customers (customer_id, customer_unique_id, zip_code_prefix, city, state)
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM staging_customers
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_unique_id = EXCLUDED.customer_unique_id,
            zip_code_prefix = EXCLUDED.zip_code_prefix,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql)
        logger.info("Populated dim_customers")

    def populate_dim_sellers(**_context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_sellers (seller_id, zip_code_prefix, city, state)
        SELECT DISTINCT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM staging_sellers
        ON CONFLICT (seller_id) DO UPDATE SET
            zip_code_prefix = EXCLUDED.zip_code_prefix,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql)
        logger.info("Populated dim_sellers")

    def populate_dim_products(**_context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_products (
            product_id,
            product_category_name,
            product_category_name_english,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            p.product_id,
            p.product_category_name,
            t.product_category_name_english,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm
        FROM staging_products p
        LEFT JOIN staging_product_category_translation t
            ON p.product_category_name = t.product_category_name
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name = EXCLUDED.product_category_name,
            product_category_name_english = EXCLUDED.product_category_name_english,
            product_weight_g = EXCLUDED.product_weight_g,
            product_length_cm = EXCLUDED.product_length_cm,
            product_height_cm = EXCLUDED.product_height_cm,
            product_width_cm = EXCLUDED.product_width_cm,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql)
        logger.info("Populated dim_products")

    def populate_dim_dates(**_context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_dates (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
            d::DATE as full_date,
            EXTRACT(YEAR FROM d)::INTEGER as year,
            EXTRACT(QUARTER FROM d)::INTEGER as quarter,
            EXTRACT(MONTH FROM d)::INTEGER as month,
            TO_CHAR(d, 'Month') as month_name,
            EXTRACT(DAY FROM d)::INTEGER as day,
            EXTRACT(DOW FROM d)::INTEGER as day_of_week,
            TO_CHAR(d, 'Day') as day_name,
            CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
        FROM generate_series(
            '2015-01-01'::DATE,
            '2030-12-31'::DATE,
            '1 day'::INTERVAL
        ) d
        ON CONFLICT (date_key) DO NOTHING;
        """
        hook.run(sql)
        logger.info("Populated dim_dates")

    def populate_all_dims(**_context):
        populate_dim_customers()
        populate_dim_sellers()
        populate_dim_products()
        populate_dim_dates()

    populate_dims = PythonOperator(
        task_id="populate_dims",
        python_callable=populate_all_dims,
    )

    def _date_key(expr: str) -> str:
        return f"CASE WHEN {expr} IS NOT NULL THEN TO_CHAR({expr}::DATE, 'YYYYMMDD')::INTEGER END"

    def populate_fact_order_items(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_date = _resolve_run_date(context)
        sql = f"""
        INSERT INTO fact_order_items (
            order_id,
            order_item_id,
            customer_key,
            seller_key,
            product_key,
            order_status,
            order_purchase_date_key,
            order_approved_date_key,
            delivered_carrier_date_key,
            delivered_customer_date_key,
            estimated_delivery_date_key,
            shipping_limit_date_key,
            price,
            freight_value,
            load_date
        )
        SELECT
            oi.order_id,
            oi.order_item_id,
            dc.customer_key,
            ds.seller_key,
            dp.product_key,
            o.order_status,
            {_date_key('o.order_purchase_timestamp')},
            {_date_key('o.order_approved_at')},
            {_date_key('o.order_delivered_carrier_date')},
            {_date_key('o.order_delivered_customer_date')},
            {_date_key('o.order_estimated_delivery_date')},
            {_date_key('oi.shipping_limit_date')},
            oi.price,
            oi.freight_value,
            %(load_date)s::DATE
        FROM staging_order_items oi
        INNER JOIN staging_orders o ON o.order_id = oi.order_id
        INNER JOIN dim_customers dc ON dc.customer_id = o.customer_id
        INNER JOIN dim_sellers ds ON ds.seller_id = oi.seller_id
        LEFT JOIN dim_products dp ON dp.product_id = oi.product_id
        ON CONFLICT (order_id, order_item_id) DO UPDATE SET
            customer_key = EXCLUDED.customer_key,
            seller_key = EXCLUDED.seller_key,
            product_key = EXCLUDED.product_key,
            order_status = EXCLUDED.order_status,
            order_purchase_date_key = EXCLUDED.order_purchase_date_key,
            order_approved_date_key = EXCLUDED.order_approved_date_key,
            delivered_carrier_date_key = EXCLUDED.delivered_carrier_date_key,
            delivered_customer_date_key = EXCLUDED.delivered_customer_date_key,
            estimated_delivery_date_key = EXCLUDED.estimated_delivery_date_key,
            shipping_limit_date_key = EXCLUDED.shipping_limit_date_key,
            price = EXCLUDED.price,
            freight_value = EXCLUDED.freight_value,
            load_date = EXCLUDED.load_date,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql, parameters={"load_date": run_date.isoformat()})
        logger.info("Populated fact_order_items (run_date=%s)", run_date)

    def populate_fact_payments(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_date = _resolve_run_date(context)
        sql = f"""
        INSERT INTO fact_payments (
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            customer_key,
            order_purchase_date_key,
            load_date
        )
        SELECT
            p.order_id,
            p.payment_sequential,
            p.payment_type,
            p.payment_installments,
            p.payment_value,
            dc.customer_key,
            {_date_key('o.order_purchase_timestamp')},
            %(load_date)s::DATE
        FROM staging_order_payments p
        INNER JOIN staging_orders o ON o.order_id = p.order_id
        INNER JOIN dim_customers dc ON dc.customer_id = o.customer_id
        ON CONFLICT (order_id, payment_sequential) DO UPDATE SET
            payment_type = EXCLUDED.payment_type,
            payment_installments = EXCLUDED.payment_installments,
            payment_value = EXCLUDED.payment_value,
            customer_key = EXCLUDED.customer_key,
            order_purchase_date_key = EXCLUDED.order_purchase_date_key,
            load_date = EXCLUDED.load_date,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql, parameters={"load_date": run_date.isoformat()})
        logger.info("Populated fact_payments (run_date=%s)", run_date)

    populate_fact_items = PythonOperator(
        task_id="populate_fact_order_items",
        python_callable=populate_fact_order_items,
    )

    populate_fact_payments_task = PythonOperator(
        task_id="populate_fact_payments",
        python_callable=populate_fact_payments,
    )

    def run_data_quality_checks(**_context):
        """Basic data quality checks: non-emptiness and key consistency."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")

        checks = [
            {
                "name": "fact_order_items_not_empty",
                "sql": "SELECT COUNT(*) FROM fact_order_items;",
                "expect": lambda v: v > 0,
            },
            {
                "name": "fact_payments_not_empty",
                "sql": "SELECT COUNT(*) FROM fact_payments;",
                "expect": lambda v: v > 0,
            },
            {
                "name": "order_items_no_null_fk",
                "sql": """
                    SELECT COUNT(*) FROM fact_order_items
                    WHERE customer_key IS NULL OR seller_key IS NULL OR product_key IS NULL;
                """,
                "expect": lambda v: v == 0,
            },
            {
                "name": "order_items_unique",
                "sql": """
                    SELECT COUNT(*) - COUNT(DISTINCT (order_id, order_item_id)) FROM fact_order_items;
                """,
                "expect": lambda v: v == 0,
            },
            {
                "name": "order_status_valid",
                "sql": """
                    SELECT COUNT(*) FROM fact_order_items
                    WHERE order_status NOT IN (
                        'approved','canceled','delivered','invoiced','processing','shipped','created','unavailable'
                    );
                """,
                "expect": lambda v: v == 0,
            },
        ]

        failures = []
        for check in checks:
            rows = hook.get_first(check["sql"])
            value = rows[0] if rows else None
            ok = check["expect"](value) if value is not None else False
            logger.info("DQ %s result=%s ok=%s", check["name"], value, ok)
            if not ok:
                failures.append(f"{check['name']} failed (value={value})")

        if failures:
            raise ValueError("Data quality checks failed: " + "; ".join(failures))

    data_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    # DAG dependencies
    ensure_data >> fail_switch >> create_staging >> load_tasks >> create_dw >> populate_dims >> [populate_fact_items, populate_fact_payments_task] >> data_quality
