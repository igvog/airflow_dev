"""
ETL DAG: GoREST API -> Postgres Staging -> Star Schema DW
Author: <your name>
"""

from __future__ import annotations

from datetime import datetime, timedelta, date
import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule


# ---------------- Logging ----------------
logger = logging.getLogger("gorest_etl")
logger.setLevel(logging.INFO)


# ---------------- Failure alert callback ----------------
def on_task_failure(context):
    ti = context.get("ti")
    dag_id = context.get("dag").dag_id
    task_id = ti.task_id if ti else "unknown"
    run_id = context.get("run_id")
    exc = context.get("exception")
    logger.error(
        f"[ALERT] DAG={dag_id} TASK={task_id} RUN={run_id} failed: {exc}"
    )
    # Здесь можно подключить email/Slack при наличии настроек
    # (SMTP/Slack connection). Для учебного проекта достаточно лога.


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_task_failure,
}

BASE_URL = "https://gorest.co.in/public/v2"
PER_PAGE = 100


# ---------------- Helpers ----------------
def _get_run_date(context) -> date:
    """
    Определяет дату запуска:
    1) dagrun.conf.run_date (YYYY-MM-DD), если передали вручную
    2) execution_date (ds) иначе
    """
    dagrun = context.get("dag_run")
    conf = dagrun.conf if dagrun else {}
    if conf and conf.get("run_date"):
        return datetime.strptime(conf["run_date"], "%Y-%m-%d").date()
    return context["execution_date"].date()


def fetch_all(endpoint: str) -> List[Dict[str, Any]]:
    """
    Постранично достаём всё из GoREST.
    Защита от 429 + базовый retry.
    """
    results = []
    page = 1
    while True:
        url = f"{BASE_URL}/{endpoint}"
        params = {"page": page, "per_page": PER_PAGE}

        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                # rate-limit: ждём немного и повторяем страницу
                time.sleep(2)
                continue

            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.exception(f"Fetch failed endpoint={endpoint} page={page}: {e}")
            raise

        if not data:
            break

        results.extend(data)
        logger.info(f"Fetched {len(data)} rows from {endpoint}, page={page}")
        page += 1

    logger.info(f"Total fetched from {endpoint}: {len(results)}")
    return results


# ---------------- DAG ----------------
with DAG(
    dag_id="gorest_to_dw_star_schema",
    description="ETL: GoREST API -> Postgres -> Star Schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["etl", "api", "gorest", "datawarehouse", "star-schema"],
    max_active_runs=1,
) as dag:

    # ========== 1) STAGING DDL ==========
    def create_staging_tables(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        DROP TABLE IF EXISTS staging_users CASCADE;
        DROP TABLE IF EXISTS staging_posts CASCADE;
        DROP TABLE IF EXISTS staging_comments CASCADE;

        CREATE TABLE staging_users (
            id        INTEGER PRIMARY KEY,
            name      TEXT,
            email     TEXT,
            gender    TEXT,
            status    TEXT,
            raw_data  JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE staging_posts (
            id        INTEGER PRIMARY KEY,
            user_id   INTEGER,
            title     TEXT,
            body      TEXT,
            raw_data  JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE staging_comments (
            id        INTEGER PRIMARY KEY,
            post_id   INTEGER,
            name      TEXT,
            email     TEXT,
            body      TEXT,
            raw_data  JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            hook.run(sql)
            logger.info("Staging tables created.")
        except Exception as e:
            logger.exception(f"Create staging failed: {e}")
            raise

    create_staging = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables,
    )

    # ========== 2) EXTRACT ==========
    def extract_gorest(**context):
        try:
            users = fetch_all("users")
            posts = fetch_all("posts")
            comments = fetch_all("comments")

            ti = context["ti"]
            ti.xcom_push(key="users", value=users)
            ti.xcom_push(key="posts", value=posts)
            ti.xcom_push(key="comments", value=comments)

            logger.info(
                f"Extract done: users={len(users)}, posts={len(posts)}, comments={len(comments)}"
            )
        except Exception as e:
            logger.exception(f"Extract failed: {e}")
            raise

    extract_task = PythonOperator(
        task_id="extract_gorest",
        python_callable=extract_gorest,
    )

    # ========== 3) LOAD TO STAGING ==========
    def load_users(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        users = context["ti"].xcom_pull(task_ids="extract_gorest", key="users") or []
        sql = """
        INSERT INTO staging_users (id, name, email, gender, status, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            gender = EXCLUDED.gender,
            status = EXCLUDED.status,
            raw_data = EXCLUDED.raw_data,
            loaded_at = CURRENT_TIMESTAMP;
        """
        try:
            for u in users:
                hook.run(
                    sql,
                    parameters=(
                        u["id"], u.get("name"), u.get("email"),
                        u.get("gender"), u.get("status"),
                        json.dumps(u),
                    ),
                )
            logger.info(f"Loaded users to staging: {len(users)}")
        except Exception as e:
            logger.exception(f"Load users failed: {e}")
            raise

    def load_posts(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        posts = context["ti"].xcom_pull(task_ids="extract_gorest", key="posts") or []
        sql = """
        INSERT INTO staging_posts (id, user_id, title, body, raw_data)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            title = EXCLUDED.title,
            body = EXCLUDED.body,
            raw_data = EXCLUDED.raw_data,
            loaded_at = CURRENT_TIMESTAMP;
        """
        try:
            for p in posts:
                hook.run(
                    sql,
                    parameters=(
                        p["id"], p.get("user_id"), p.get("title"),
                        p.get("body"), json.dumps(p),
                    ),
                )
            logger.info(f"Loaded posts to staging: {len(posts)}")
        except Exception as e:
            logger.exception(f"Load posts failed: {e}")
            raise

    def load_comments(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        comments = context["ti"].xcom_pull(task_ids="extract_gorest", key="comments") or []
        sql = """
        INSERT INTO staging_comments (id, post_id, name, email, body, raw_data)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE SET
            post_id = EXCLUDED.post_id,
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            body = EXCLUDED.body,
            raw_data = EXCLUDED.raw_data,
            loaded_at = CURRENT_TIMESTAMP;
        """
        try:
            for c in comments:
                hook.run(
                    sql,
                    parameters=(
                        c["id"], c.get("post_id"), c.get("name"),
                        c.get("email"), c.get("body"), json.dumps(c),
                    ),
                )
            logger.info(f"Loaded comments to staging: {len(comments)}")
        except Exception as e:
            logger.exception(f"Load comments failed: {e}")
            raise

    load_users_task = PythonOperator(
        task_id="load_users_to_staging",
        python_callable=load_users,
    )
    load_posts_task = PythonOperator(
        task_id="load_posts_to_staging",
        python_callable=load_posts,
    )
    load_comments_task = PythonOperator(
        task_id="load_comments_to_staging",
        python_callable=load_comments,
    )

    # ========== 4) CREATE DW SCHEMA ==========
    create_dw_schema = PostgresOperator(
        task_id="create_dw_schema",
        postgres_conn_id="postgres_etl_target_conn",
        sql="""
        DROP TABLE IF EXISTS fact_posts CASCADE;
        DROP TABLE IF EXISTS dim_users CASCADE;
        DROP TABLE IF EXISTS dim_dates CASCADE;

        CREATE TABLE dim_users (
            user_key      SERIAL PRIMARY KEY,
            user_id       INTEGER UNIQUE NOT NULL,
            name          VARCHAR(200),
            email         VARCHAR(200),
            gender        VARCHAR(20),
            status        VARCHAR(20),
            valid_from    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to      TIMESTAMP,
            is_current    BOOLEAN DEFAULT TRUE,
            updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_dim_users_user_id ON dim_users(user_id);

        CREATE TABLE dim_dates (
            date_key     INTEGER PRIMARY KEY,
            full_date    DATE NOT NULL,
            year         SMALLINT NOT NULL,
            quarter      SMALLINT NOT NULL,
            month        SMALLINT NOT NULL,
            month_name   VARCHAR(20) NOT NULL,
            day          SMALLINT NOT NULL,
            day_of_week  SMALLINT NOT NULL,
            day_name     VARCHAR(20) NOT NULL,
            is_weekend   BOOLEAN NOT NULL
        );

        CREATE TABLE fact_posts (
            post_key      SERIAL PRIMARY KEY,
            post_id       INTEGER NOT NULL,
            user_key      INTEGER NOT NULL REFERENCES dim_users(user_key),
            date_key      INTEGER NOT NULL REFERENCES dim_dates(date_key),
            title         TEXT,
            body          TEXT,
            body_length   INTEGER,
            word_count    INTEGER,
            comment_count INTEGER DEFAULT 0,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_fact_posts UNIQUE(post_id)
        );
        CREATE INDEX idx_fact_posts_user_key ON fact_posts(user_key);
        CREATE INDEX idx_fact_posts_date_key ON fact_posts(date_key);
        CREATE INDEX idx_fact_posts_post_id  ON fact_posts(post_id);
        """,
    )

    # ========== 5) POPULATE DIMENSIONS ==========
    def populate_dim_users(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_users (user_id, name, email, gender, status)
        SELECT DISTINCT
            id, name, email, gender, status
        FROM staging_users
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            gender = EXCLUDED.gender,
            status = EXCLUDED.status,
            updated_at = CURRENT_TIMESTAMP;
        """
        try:
            hook.run(sql)
            logger.info("dim_users populated.")
        except Exception as e:
            logger.exception(f"Populate dim_users failed: {e}")
            raise

    populate_dim_users_task = PythonOperator(
        task_id="populate_dim_users",
        python_callable=populate_dim_users,
    )

    def populate_dim_dates(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO dim_dates (date_key, full_date, year, quarter, month, month_name,
                               day, day_of_week, day_name, is_weekend)
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
            d::DATE as full_date,
            EXTRACT(YEAR FROM d)::SMALLINT,
            EXTRACT(QUARTER FROM d)::SMALLINT,
            EXTRACT(MONTH FROM d)::SMALLINT,
            TO_CHAR(d, 'Month')::VARCHAR(20),
            EXTRACT(DAY FROM d)::SMALLINT,
            EXTRACT(DOW FROM d)::SMALLINT,
            TO_CHAR(d, 'Day')::VARCHAR(20),
            CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN TRUE ELSE FALSE END
        FROM generate_series('2020-01-01'::DATE, '2027-12-31'::DATE, '1 day') d
        ON CONFLICT (date_key) DO NOTHING;
        """
        try:
            hook.run(sql)
            logger.info("dim_dates populated.")
        except Exception as e:
            logger.exception(f"Populate dim_dates failed: {e}")
            raise

    populate_dim_dates_task = PythonOperator(
        task_id="populate_dim_dates",
        python_callable=populate_dim_dates,
    )

    # ========== 6) POPULATE FACT ==========
    def populate_fact_posts(**context):
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        run_dt = _get_run_date(context)
        run_date_key = int(run_dt.strftime("%Y%m%d"))

        # backfill/refill опция
        reset = False
        dagrun = context.get("dag_run")
        if dagrun and dagrun.conf and dagrun.conf.get("reset") is True:
            reset = True

        try:
            if reset:
                hook.run(
                    "DELETE FROM fact_posts WHERE date_key = %s;",
                    parameters=(run_date_key,),
                )
                logger.info(f"Reset partition for date_key={run_date_key}")

            sql = """
            INSERT INTO fact_posts (post_id, user_key, date_key, title, body,
                                    body_length, word_count, comment_count)
            SELECT
                sp.id AS post_id,
                du.user_key,
                %s::INTEGER AS date_key,
                sp.title,
                sp.body,
                LENGTH(sp.body) AS body_length,
                array_length(string_to_array(sp.body, ' '), 1) AS word_count,
                COALESCE(cc.comment_count, 0) AS comment_count
            FROM staging_posts sp
            JOIN dim_users du ON du.user_id = sp.user_id
            LEFT JOIN (
                SELECT post_id, COUNT(*) AS comment_count
                FROM staging_comments
                GROUP BY post_id
            ) cc ON cc.post_id = sp.id
            ON CONFLICT (post_id) DO UPDATE SET
                user_key = EXCLUDED.user_key,
                date_key = EXCLUDED.date_key,
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                body_length = EXCLUDED.body_length,
                word_count = EXCLUDED.word_count,
                comment_count = EXCLUDED.comment_count,
                updated_at = CURRENT_TIMESTAMP;
            """
            hook.run(sql, parameters=(run_date_key,))
            logger.info(f"fact_posts populated for date_key={run_date_key}")
        except Exception as e:
            logger.exception(f"Populate fact_posts failed: {e}")
            raise

    populate_fact_posts_task = PythonOperator(
        task_id="populate_fact_posts",
        python_callable=populate_fact_posts,
    )

    # ========== DAG DEPENDENCIES ==========
    create_staging >> extract_task
    extract_task >> [load_users_task, load_posts_task, load_comments_task]
    [load_users_task, load_posts_task, load_comments_task] >> create_dw_schema
    create_dw_schema >> [populate_dim_users_task, populate_dim_dates_task]
    [populate_dim_users_task, populate_dim_dates_task] >> populate_fact_posts_task
