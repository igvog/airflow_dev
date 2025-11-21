"""
ETL DAG: API to Data Warehouse Star Schema
This DAG extracts data from JSONPlaceholder API, loads it into Postgres,
and transforms it into a star schema data warehouse model.
The DAG is parameterized to enable backfills or re-runs for any date.
"""

from datetime import timedelta
import json
import logging
import os
from typing import List, Optional

import pendulum
import requests
from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

logger = logging.getLogger("api_to_dw_star_schema")
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


def _send_telegram(message: str):
    """Send a Telegram message if credentials are configured."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("Telegram not configured; skipping alert")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    try:
        resp = requests.post(url, json=payload, timeout=15)
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Failed to send Telegram alert: %s", exc)


def _alert_on_failure(context):
    """Send alerts (email + Telegram) if any task fails."""
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
    _send_telegram(f"[Airflow][{dag_id}] Task failed: {task_id} (run_id={run_id})\\nLog: {log_url}")


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


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _alert_on_failure,
}

# DAG definition
with DAG(
    "api_to_dw_star_schema",
    default_args=default_args,
    description="ETL pipeline: API -> Postgres -> Star Schema DW",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["etl", "api", "datawarehouse", "star-schema"],
    params={
        "run_date": Param(
            default=None,
            type=["null", "string"],
            description="Override logical load date, e.g. 2024-01-01 (otherwise uses data interval end).",
        ),
        "full_refresh": Param(
            default=True,
            type="boolean",
            description="Drop and recreate staging/DW tables before load.",
        ),
    },
) as dag:

    # ========== STAGING LAYER ==========

    def create_staging_tables(**context):
        """Create staging tables for raw API data."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        full_refresh: bool = bool(context["params"].get("full_refresh"))

        drop_staging = """
        DROP TABLE IF EXISTS staging_posts CASCADE;
        DROP TABLE IF EXISTS staging_users CASCADE;
        DROP TABLE IF EXISTS staging_comments CASCADE;
        """

        create_staging = """
        CREATE TABLE IF NOT EXISTS staging_posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT,
            body TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS staging_users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            username TEXT,
            email TEXT,
            phone TEXT,
            website TEXT,
            address JSONB,
            company JSONB,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS staging_comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER,
            name TEXT,
            email TEXT,
            body TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        if full_refresh:
            hook.run(drop_staging)
        hook.run(create_staging)
        logger.info("Staging tables created successfully (full_refresh=%s)", full_refresh)

    create_staging = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables,
    )

    def fetch_api_data(**context):
        """Fetch data from JSONPlaceholder API."""
        base_url = "https://jsonplaceholder.typicode.com"
        run_date = _resolve_run_date(context)

        try:
            posts_response = requests.get(f"{base_url}/posts", timeout=30)
            posts_response.raise_for_status()
            posts_data = posts_response.json()
            logger.info("Fetched %s posts from API", len(posts_data))

            users_response = requests.get(f"{base_url}/users", timeout=30)
            users_response.raise_for_status()
            users_data = users_response.json()
            logger.info("Fetched %s users from API", len(users_data))

            comments_response = requests.get(f"{base_url}/comments", timeout=30)
            comments_response.raise_for_status()
            comments_data = comments_response.json()
            logger.info("Fetched %s comments from API", len(comments_data))

            context["ti"].xcom_push(key="posts_data", value=posts_data)
            context["ti"].xcom_push(key="users_data", value=users_data)
            context["ti"].xcom_push(key="comments_data", value=comments_data)
            context["ti"].xcom_push(key="run_date", value=run_date.isoformat())

            return {
                "posts_count": len(posts_data),
                "users_count": len(users_data),
                "comments_count": len(comments_data),
                "run_date": run_date.isoformat(),
            }
        except Exception as exc:
            logger.exception("Error fetching API data")
            raise exc

    fetch_data = PythonOperator(
        task_id="fetch_api_data",
        python_callable=fetch_api_data,
    )

    def load_posts_to_staging(**context):
        """Load posts data into staging table."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        posts_data = context["ti"].xcom_pull(key="posts_data", task_ids="fetch_api_data")
        run_date = pendulum.parse(context["ti"].xcom_pull(key="run_date")).date()

        for post in posts_data:
            insert_query = """
            INSERT INTO staging_posts (id, user_id, title, body, raw_data, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                raw_data = EXCLUDED.raw_data,
                loaded_at = EXCLUDED.loaded_at;
            """
            hook.run(
                insert_query,
                parameters=(
                    post["id"],
                    post["userId"],
                    post["title"],
                    post["body"],
                    json.dumps(post),
                    run_date,
                ),
            )

        logger.info("Loaded %s posts into staging_posts", len(posts_data))

    load_posts = PythonOperator(
        task_id="load_posts_to_staging",
        python_callable=load_posts_to_staging,
    )

    def load_users_to_staging(**context):
        """Load users data into staging table."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        users_data = context["ti"].xcom_pull(key="users_data", task_ids="fetch_api_data")
        run_date = pendulum.parse(context["ti"].xcom_pull(key="run_date")).date()

        for user in users_data:
            insert_query = """
            INSERT INTO staging_users (id, name, username, email, phone, website, address, company, raw_data, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                website = EXCLUDED.website,
                address = EXCLUDED.address,
                company = EXCLUDED.company,
                raw_data = EXCLUDED.raw_data,
                loaded_at = EXCLUDED.loaded_at;
            """
            hook.run(
                insert_query,
                parameters=(
                    user["id"],
                    user["name"],
                    user["username"],
                    user["email"],
                    user.get("phone", ""),
                    user.get("website", ""),
                    json.dumps(user.get("address", {})),
                    json.dumps(user.get("company", {})),
                    json.dumps(user),
                    run_date,
                ),
            )

        logger.info("Loaded %s users into staging_users", len(users_data))

    load_users = PythonOperator(
        task_id="load_users_to_staging",
        python_callable=load_users_to_staging,
    )

    def load_comments_to_staging(**context):
        """Load comments data into staging table."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        comments_data = context["ti"].xcom_pull(key="comments_data", task_ids="fetch_api_data")
        run_date = pendulum.parse(context["ti"].xcom_pull(key="run_date")).date()

        for comment in comments_data:
            insert_query = """
            INSERT INTO staging_comments (id, post_id, name, email, body, raw_data, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                post_id = EXCLUDED.post_id,
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                body = EXCLUDED.body,
                raw_data = EXCLUDED.raw_data,
                loaded_at = EXCLUDED.loaded_at;
            """
            hook.run(
                insert_query,
                parameters=(
                    comment["id"],
                    comment["postId"],
                    comment["name"],
                    comment["email"],
                    comment["body"],
                    json.dumps(comment),
                    run_date,
                ),
            )

        logger.info("Loaded %s comments into staging_comments", len(comments_data))

    load_comments = PythonOperator(
        task_id="load_comments_to_staging",
        python_callable=load_comments_to_staging,
    )

    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========

    def create_dw_schema(**context):
        """Create DW tables (conditionally dropping existing ones for full refresh)."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        full_refresh: bool = bool(context["params"].get("full_refresh"))

        drop_dw = """
        DROP TABLE IF EXISTS fact_posts CASCADE;
        DROP TABLE IF EXISTS dim_users CASCADE;
        DROP TABLE IF EXISTS dim_dates CASCADE;
        """

        create_dw = """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_key SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL,
            username VARCHAR(100),
            name VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            website VARCHAR(200),
            city VARCHAR(100),
            company_name VARCHAR(200),
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
            month_name VARCHAR(20),
            day INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            is_weekend BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS fact_posts (
            post_key SERIAL PRIMARY KEY,
            post_id INTEGER NOT NULL,
            user_key INTEGER REFERENCES dim_users(user_key),
            date_key INTEGER REFERENCES dim_dates(date_key),
            title TEXT,
            body TEXT,
            body_length INTEGER,
            word_count INTEGER,
            comment_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_fact_posts_user_key ON fact_posts(user_key);
        CREATE INDEX IF NOT EXISTS idx_fact_posts_date_key ON fact_posts(date_key);
        CREATE INDEX IF NOT EXISTS idx_fact_posts_post_id ON fact_posts(post_id);
        """

        if full_refresh:
            hook.run(drop_dw)
        hook.run(create_dw)
        logger.info("DW schema ready (full_refresh=%s)", full_refresh)

    create_dw_schema_task = PythonOperator(
        task_id="create_dw_schema",
        python_callable=create_dw_schema,
    )

    def populate_dim_users(**context):
        """Populate user dimension table from staging."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")

        sql = """
        INSERT INTO dim_users (user_id, username, name, email, phone, website, city, company_name)
        SELECT DISTINCT
            id as user_id,
            username,
            name,
            email,
            phone,
            website,
            address->>'city' as city,
            company->>'name' as company_name
        FROM staging_users
        ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            website = EXCLUDED.website,
            city = EXCLUDED.city,
            company_name = EXCLUDED.company_name,
            updated_at = CURRENT_TIMESTAMP;
        """

        hook.run(sql)
        logger.info("Populated dim_users dimension table")

    populate_dim_users_task = PythonOperator(
        task_id="populate_dim_users",
        python_callable=populate_dim_users,
    )

    def populate_dim_dates(**context):
        """Populate date dimension table."""
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
            '2020-01-01'::DATE,
            '2027-12-31'::DATE,
            '1 day'::INTERVAL
        ) d
        ON CONFLICT (date_key) DO NOTHING;
        """

        hook.run(sql)
        logger.info("Populated dim_dates dimension table")

    populate_dim_dates_task = PythonOperator(
        task_id="populate_dim_dates",
        python_callable=populate_dim_dates,
    )

    def populate_fact_posts(**context):
        """Populate fact table with posts data."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_date = pendulum.parse(context["ti"].xcom_pull(key="run_date")).date()

        sql = """
        MERGE INTO public.fact_posts AS e USING (SELECT
            sp.id as post_id,
            du.user_key,
            TO_CHAR(%(run_date)s::DATE, 'YYYYMMDD')::INTEGER as date_key,
            sp.title,
            sp.body,
            LENGTH(sp.body) as body_length,
            array_length(string_to_array(sp.body, ' '), 1) as word_count,
            COALESCE(comment_counts.comment_count, 0) as comment_count
        FROM staging_posts sp
        INNER JOIN dim_users du ON sp.user_id = du.user_id
        LEFT JOIN (
            SELECT post_id, COUNT(*) as comment_count
            FROM staging_comments
            GROUP BY post_id
        ) comment_counts ON sp.id = comment_counts.post_id) AS u
        ON u.post_id = e.post_id
        WHEN MATCHED THEN
            UPDATE SET  user_key = u.user_key,
            date_key = u.date_key,
            title = u.title,
            body = u.body,
            body_length = u.body_length,
            word_count = u.word_count,
            comment_count = u.comment_count,
            updated_at = CURRENT_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (post_id, user_key, date_key, title, body, body_length, word_count, comment_count)
        VALUES (u.post_id, u.user_key, u.date_key, u.title, u.body, u.body_length, u.word_count, u.comment_count);
        """

        hook.run(sql, parameters={"run_date": run_date})
        logger.info("Populated fact_posts fact table for run_date=%s", run_date)

    populate_fact_posts_task = PythonOperator(
        task_id="populate_fact_posts",
        python_callable=populate_fact_posts,
    )

    # ========== TASK DEPENDENCIES ==========

    create_staging >> fetch_data
    fetch_data >> [load_posts, load_users, load_comments]
    [load_posts, load_users, load_comments] >> create_dw_schema_task
    create_dw_schema_task >> [populate_dim_users_task, populate_dim_dates_task]
    [populate_dim_users_task, populate_dim_dates_task] >> populate_fact_posts_task
