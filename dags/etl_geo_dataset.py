import os
import json
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

TELEGRAM_TOKEN = "7989720867:AAHLRDagG2SVAAL6GamUdN4aH-Wp5VSWS-0"
CHAT_ID = "497526694"


def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, data=payload, timeout=5)
    except Exception as e:
        logger.error(f"Failed to send telegram message: {e}")


def notify_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    date = context["ds"]

    send_telegram_message(
        f"❌ TASK FAILED\nDAG: {dag_id}\nTask: {task_id}\nDate: {date}"
    )


def notify_success(context):
    dag_id = context["dag"].dag_id
    date = context["ds"]

    send_telegram_message(
        f"✅ DAG SUCCESS\nDAG: {dag_id}\nDate: {date}"
    )


ETL_POSTGRES_CONN_ID = "postgres_etl_target_conn"
DATA_DIR = "/opt/airflow/dags/json"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "on_failure_callback": notify_failure,
}


def safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


with DAG(
    dag_id="etl_geo_dataset",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["etl", "geo"],
    on_success_callback=notify_success,
) as dag:

    @task
    def discovery_files():
        files = [f for f in os.listdir(DATA_DIR) if f.endswith(".json")]
        if not files:
            raise FileNotFoundError(f"No JSON files found in {DATA_DIR}")
        logger.info(f"Discovered files: {files}")
        return files

    @task
    def create_tables():
        pg = PostgresHook(postgres_conn_id=ETL_POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()
        ddl = """
        CREATE TABLE IF NOT EXISTS dim_country (
            id INTEGER PRIMARY KEY,
            name TEXT,
            iso2 TEXT,
            iso3 TEXT,
            phone_code TEXT,
            capital TEXT,
            region TEXT,
            subregion TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
        CREATE TABLE IF NOT EXISTS dim_state (
            id INTEGER PRIMARY KEY,
            country_id INTEGER,
            name TEXT,
            state_code TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
        CREATE TABLE IF NOT EXISTS dim_city (
            id INTEGER PRIMARY KEY,
            state_id INTEGER,
            country_id INTEGER,
            name TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
        CREATE TABLE IF NOT EXISTS fact_location (
            city_id INTEGER,
            state_id INTEGER,
            country_id INTEGER,
            run_date DATE
        );
        """
        cur.execute(ddl)
        conn.commit()
        cur.close()
        logger.info("Tables created successfully.")

    @task
    def load_data(json_files: list):
        pg = PostgresHook(postgres_conn_id=ETL_POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        for file in json_files:
            file_path = os.path.join(DATA_DIR, file)
            logger.info(f"Processing file: {file_path}")
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data = [data]

            for row in data:
                try:
                    if "id" not in row:
                        continue
                    if "state_id" in row and "country_id" in row:
                        cur.execute("""
                            INSERT INTO dim_city (id, state_id, country_id, name, latitude, longitude)
                            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING
                        """, (row.get("id"), row.get("state_id"), row.get("country_id"),
                              row.get("name"), safe_float(row.get("latitude")), safe_float(row.get("longitude"))))
                    elif "country_id" in row:
                        cur.execute("""
                            INSERT INTO dim_state (id, country_id, name, state_code, latitude, longitude)
                            VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING
                        """, (row.get("id"), row.get("country_id"), row.get("name"),
                              row.get("state_code"), safe_float(row.get("latitude")), safe_float(row.get("longitude"))))
                    else:
                        cur.execute("""
                            INSERT INTO dim_country (id, name, iso2, iso3, phone_code, capital, region, subregion, latitude, longitude)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING
                        """, (row.get("id"), row.get("name"), row.get("iso2"), row.get("iso3"),
                              row.get("phone_code"), row.get("capital"), row.get("region"),
                              row.get("subregion"), safe_float(row.get("latitude")), safe_float(row.get("longitude"))))
                except Exception as e:
                    logger.error(f"Error inserting row {row}: {e}")

        conn.commit()
        cur.close()
        logger.info("Data loaded successfully.")

    @task
    def build_fact():
        context = get_current_context()
        run_date_str = context["ds"]
        run_date = datetime.strptime(run_date_str, "%Y-%m-%d").date()
        pg = PostgresHook(postgres_conn_id=ETL_POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM dim_city")
        count = cur.fetchone()[0]
        if count == 0:
            raise ValueError("dim_city is empty, cannot build fact_location")

        cur.execute("""
            INSERT INTO fact_location (city_id, state_id, country_id, run_date)
            SELECT id, state_id, country_id, %s
            FROM dim_city
        """, (run_date,))

        conn.commit()
        cur.close()
        logger.info(f"Fact table populated successfully for run_date={run_date}")

    files = discovery_files()
    create = create_tables()
    load = load_data(files)
    fact = build_fact()

    create >> files >> load >> fact
