"""
Refactored DAG: Kaggle CSV -> Postgres DW (star schema)
- robust, idempotent, handles European CSV (delimiter=';', price with comma, dd.mm.YYYY HH:MM)
- creates ~/.kaggle/kaggle.json from env vars
- uses PostgresHook for DB ops and psycopg2.execute_values for fast bulk load
- avoids ON CONFLICT "affect row a second time" by aggregating/grouping / SELECT DISTINCT
- params: full_refresh (bool) via dag_run.conf
"""

from datetime import datetime, timedelta
import os
import json
import logging
import csv
import datetime as dt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Optional: change to your connection id
POSTGRES_CONN_ID = "postgres_etl_target_conn"

# Dataset info
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "aslanahmedov/market-basket-analysis")
CSV_NAME = "Assignment-1_Data.csv"
DOWNLOAD_DIR = "/tmp/kaggle_download"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def send_failure_alert(context):
    logging.error("Task failed: %s", context.get("exception"))


def prepare_kaggle_credentials():
    """
    Create ~/.kaggle/kaggle.json from env vars KAGGLE_USERNAME and KAGGLE_KEY
    """
    username = os.getenv("KAGGLE_USERNAME")
    key = os.getenv("KAGGLE_KEY")
    if not username or not key:
        raise ValueError("KAGGLE_USERNAME and KAGGLE_KEY must be set in env")

    kaggle_dir = "/home/airflow/.kaggle"
    os.makedirs(kaggle_dir, exist_ok=True)
    path = os.path.join(kaggle_dir, "kaggle.json")
    with open(path, "w") as f:
        json.dump({"username": username, "key": key}, f)
    os.chmod(path, 0o600)
    logging.info("Wrote kaggle credentials to %s", path)


def download_from_kaggle(**context):
    """
    Download dataset using kaggle CLI into DOWNLOAD_DIR and push csv_path to XCom.
    If local file exists in /opt/airflow/data/Assignment-1_Data.csv, prefer it (convenience).
    """
    import subprocess

    local_path = f"/opt/airflow/data/{CSV_NAME}"
    if os.path.exists(local_path):
        context['ti'].xcom_push(key="csv_path", value=local_path)
        logging.info("Found local CSV at %s — skipping Kaggle download", local_path)
        return local_path

    prepare_kaggle_credentials()

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    cmd = [
        "kaggle", "datasets", "download",
        "-d", KAGGLE_DATASET,
        "-p", DOWNLOAD_DIR,
        "--unzip",
        "--force"
    ]
    logging.info("Running command: %s", " ".join(cmd))
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        logging.error("Kaggle download failed: %s", e)
        raise

    # find csv
    csv_path = os.path.join(DOWNLOAD_DIR, CSV_NAME)
    if not os.path.exists(csv_path):
        # search nested
        for root, _, files in os.walk(DOWNLOAD_DIR):
            if CSV_NAME in files:
                csv_path = os.path.join(root, CSV_NAME)
                break

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{CSV_NAME} not found in {DOWNLOAD_DIR}")

    context['ti'].xcom_push(key="csv_path", value=csv_path)
    logging.info("CSV downloaded to %s", csv_path)
    return csv_path


def _create_tables():
    """
    Create staging and DW tables (idempotent). Uses simple schemas similar to earlier discussion.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = """
    -- staging
    CREATE TABLE IF NOT EXISTS staging_sales (
        bill_no TEXT,
        item_name TEXT,
        quantity NUMERIC,
        txn_ts TIMESTAMP,
        price NUMERIC,
        customer_id TEXT,
        country TEXT,
        raw_row JSONB,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- dims
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_key SERIAL PRIMARY KEY,
        customer_id TEXT UNIQUE NOT NULL,
        country TEXT,
        valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dim_items (
        item_key SERIAL PRIMARY KEY,
        item_name TEXT UNIQUE NOT NULL,
        avg_price NUMERIC,
        valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN DEFAULT TRUE,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS dim_dates (
        date_key INTEGER PRIMARY KEY,
        full_date DATE NOT NULL,
        year INTEGER,
        quarter INTEGER,
        month INTEGER,
        day INTEGER,
        day_of_week INTEGER,
        is_weekend BOOLEAN
    );

    CREATE TABLE IF NOT EXISTS fact_sales (
        sale_key SERIAL PRIMARY KEY,
        bill_no TEXT,
        item_key INTEGER REFERENCES dim_items(item_key),
        customer_key INTEGER REFERENCES dim_customers(customer_key),
        date_key INTEGER REFERENCES dim_dates(date_key),
        quantity NUMERIC,
        price NUMERIC,
        total_amount NUMERIC,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- uniqueness to support idempotency (bill_no + item_key)
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uniq_bill_item'
      ) THEN
        ALTER TABLE fact_sales ADD CONSTRAINT uniq_bill_item UNIQUE (bill_no, item_key);
      END IF;
    END$$;
    """
    hook.run(sql)
    logging.info("Ensured staging/dim/fact tables exist")


def create_staging_and_dw(**context):
    _create_tables()


def load_csv_to_staging(**context):
    """
    Read CSV (delimiter=';'), normalize columns, convert price and date, then bulk insert into staging_sales.
    Uses psycopg2.extras.execute_values for speed.
    """
    from psycopg2.extras import execute_values
    import psycopg2
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    csv_path = context['ti'].xcom_pull(key="csv_path", task_ids="download_from_kaggle")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError("csv_path XCom missing or file not found")

    rows = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        # normalize fieldnames (strip, remove BOM)
        reader.fieldnames = [fn.strip().replace("\ufeff", "") for fn in reader.fieldnames]
        for raw in reader:
            r = {k.strip().replace("\ufeff", ""): v for k, v in raw.items()}
            # required fields tolerance
            bill = r.get("BillNo") or r.get("BillNo".lower()) or r.get("Bill No") or None
            item = r.get("Itemname") or r.get("ItemName") or r.get("itemname") or None
            qty = r.get("Quantity") or r.get("quantity") or None
            date_raw = r.get("Date") or r.get("date") or None
            price_raw = r.get("Price") or r.get("price") or None
            cust = r.get("CustomerID") or r.get("CustomerId") or r.get("customerid") or None
            country = r.get("Country") or r.get("country") or None

            if not bill or not item:
                # skip malformed
                continue

            # normalize quantity
            try:
                quantity = float(qty) if qty not in (None, "") else None
            except Exception:
                quantity = None

            # normalize price: "2,55" -> "2.55"
            price = None
            if price_raw:
                price = price_raw.replace(",", ".")
                try:
                    price = float(price)
                except Exception:
                    price = None

            # normalize date: "01.12.2010 08:26" -> datetime
            txn_ts = None
            if date_raw:
                try:
                    txn_ts = dt.datetime.strptime(date_raw.strip(), "%d.%m.%Y %H:%M")
                except Exception:
                    # try alternate formats
                    try:
                        txn_ts = dt.datetime.fromisoformat(date_raw.strip())
                    except Exception:
                        txn_ts = None

            raw_json = json.dumps(r, default=str)
            rows.append((
                bill, item.strip() if isinstance(item, str) else item,
                quantity, txn_ts, price, cust, country, raw_json
            ))

    if not rows:
        logging.warning("No rows parsed from CSV")
        context['ti'].xcom_push(key='staging_rows', value=0)
        return

    # insert
    insert_sql = """
    INSERT INTO staging_sales
      (bill_no, item_name, quantity, txn_ts, price, customer_id, country, raw_row)
    VALUES %s
    """
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()
        logging.info("Inserted %s rows into staging_sales", len(rows))
    except Exception as e:
        conn.rollback()
        logging.exception("Failed bulk insert into staging_sales: %s", e)
        raise
    finally:
        cur.close()
        conn.close()

    context['ti'].xcom_push(key='staging_rows', value=len(rows))


def populate_dim_and_fact(**context):
    """
    Robust population of dims and fact:
    - dim_customers: GROUP BY customer_id -> single country (MAX)
    - dim_items: GROUP BY TRIM(item_name) -> avg price
    - dim_dates: GROUP BY DATE(txn_ts)
    - fact_sales: build distinct src and insert ON CONFLICT update
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    dag_conf = context.get("dag_run").conf if context.get("dag_run") else {}
    full_refresh = bool(dag_conf.get("full_refresh", False))

    if full_refresh:
        hook.run("TRUNCATE TABLE fact_sales CASCADE;")
        logging.info("Truncated fact_sales for full refresh")

    # dim_customers
    sql_customers = """
    INSERT INTO dim_customers (customer_id, country)
    SELECT customer_id, MAX(country) AS country
    FROM staging_sales
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
    ON CONFLICT (customer_id)
    DO UPDATE SET country = EXCLUDED.country, updated_at = CURRENT_TIMESTAMP;
    """
    hook.run(sql_customers)
    logging.info("Upserted dim_customers")

    # dim_items
    sql_items = """
    INSERT INTO dim_items (item_name, avg_price)
    SELECT TRIM(item_name) AS item_name, AVG(price::NUMERIC) AS avg_price
    FROM staging_sales
    WHERE item_name IS NOT NULL AND item_name <> ''
      AND price IS NOT NULL
    GROUP BY TRIM(item_name)
    ON CONFLICT (item_name) DO UPDATE
      SET avg_price = EXCLUDED.avg_price, updated_at = CURRENT_TIMESTAMP;
    """
    hook.run(sql_items)
    logging.info("Upserted dim_items")

    # dim_dates
    sql_dates = """
    INSERT INTO dim_dates (date_key, full_date, year, quarter, month, day, day_of_week, is_weekend)
    SELECT
      TO_CHAR(DATE(txn_ts), 'YYYYMMDD')::INTEGER AS date_key,
      DATE(txn_ts) AS full_date,
      EXTRACT(YEAR FROM DATE(txn_ts))::INTEGER AS year,
      EXTRACT(QUARTER FROM DATE(txn_ts))::INTEGER AS quarter,
      EXTRACT(MONTH FROM DATE(txn_ts))::INTEGER AS month,
      EXTRACT(DAY FROM DATE(txn_ts))::INTEGER AS day,
      EXTRACT(DOW FROM DATE(txn_ts))::INTEGER AS day_of_week,
      CASE WHEN EXTRACT(DOW FROM DATE(txn_ts)) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM staging_sales
    WHERE txn_ts IS NOT NULL
    GROUP BY DATE(txn_ts)
    ON CONFLICT (date_key) DO NOTHING;
    """
    hook.run(sql_dates)
    logging.info("Populated dim_dates")

    # Ensure unique constraint exists (idempotent)
    hook.run("ALTER TABLE fact_sales DROP CONSTRAINT IF EXISTS uniq_bill_item;")
    hook.run("""
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_bill_item') THEN
        ALTER TABLE fact_sales ADD CONSTRAINT uniq_bill_item UNIQUE (bill_no, item_key);
      END IF;
    END$$;
    """)
    logging.info("Ensured unique constraint on fact_sales(bill_no, item_key)")

    # Merge into fact_sales using DISTINCT src to avoid duplicate keys in single INSERT
    merge_sql = """
    WITH src AS (
    SELECT
        s.bill_no,
        di.item_key,
        dc.customer_key,
        TO_CHAR(DATE(s.txn_ts), 'YYYYMMDD')::INTEGER AS date_key,
        SUM(s.quantity)::NUMERIC AS quantity,
        AVG(s.price)::NUMERIC AS price,
        SUM(s.quantity * s.price)::NUMERIC AS total_amount
    FROM staging_sales s
    LEFT JOIN dim_items di ON TRIM(s.item_name) = di.item_name
    LEFT JOIN dim_customers dc ON s.customer_id = dc.customer_id
    WHERE s.bill_no IS NOT NULL
    GROUP BY
        s.bill_no, di.item_key, dc.customer_key, DATE(s.txn_ts)
)
    INSERT INTO fact_sales (bill_no, item_key, customer_key, date_key, quantity, price, total_amount)
    SELECT bill_no, item_key, customer_key, date_key, quantity, price, total_amount
    FROM src
    WHERE item_key IS NOT NULL AND customer_key IS NOT NULL AND date_key IS NOT NULL
    ON CONFLICT (bill_no, item_key) DO UPDATE
      SET customer_key = EXCLUDED.customer_key,
          date_key = EXCLUDED.date_key,
          quantity = EXCLUDED.quantity,
          price = EXCLUDED.price,
          total_amount = EXCLUDED.total_amount,
          updated_at = CURRENT_TIMESTAMP;
    """
    hook.run(merge_sql)
    logging.info("Merged staging into fact_sales")


def validate_row_counts(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    staging = hook.get_first("SELECT COUNT(1) FROM staging_sales")[0]
    facts = hook.get_first("SELECT COUNT(1) FROM fact_sales")[0]
    logging.info("Row counts — staging: %s, fact: %s", staging, facts)
    if staging == 0:
        raise ValueError("No rows loaded into staging_sales — aborting pipeline")


with DAG(
    "kaggle_sales_to_dw_refactor",
    default_args=default_args,
    description="Refactored: Kaggle CSV → Postgres DW",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["kaggle", "etl", "dw"],
    on_failure_callback=send_failure_alert,
) as dag:

    t1_download = PythonOperator(
        task_id="download_from_kaggle",
        python_callable=download_from_kaggle,
        provide_context=True,
    )

    t2_create = PythonOperator(
        task_id="create_staging_and_dw",
        python_callable=create_staging_and_dw,
    )

    t3_load_staging = PythonOperator(
        task_id="load_csv_to_staging",
        python_callable=load_csv_to_staging,
    )

    t4_populate = PythonOperator(
        task_id="populate_dim_and_fact",
        python_callable=populate_dim_and_fact,
    )

    t5_validate = PythonOperator(
        task_id="validate_row_counts",
        python_callable=validate_row_counts,
    )

    t1_download >> t2_create >> t3_load_staging >> t4_populate >> t5_validate
