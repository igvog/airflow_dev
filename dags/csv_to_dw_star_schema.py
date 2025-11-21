from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
import pandas as pd
import logging
import requests
from datetime import datetime, timedelta

# --------------------
# Telegram
# --------------------
TELEGRAM_TOKEN = "8511023105:AAF-CAtsymYKjP1-I5IF5TXwgioUjHlay1o"
CHAT_ID = "719301454"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# --------------------
# Failure callback
# --------------------
def notify_failure(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    message = f"❌ DAG: {dag_id}, Task: {task_id} FAILED on {execution_date}"
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, data=payload)
    except Exception as e:
        logging.error(f"Failed to send telegram message: {e}")

# --------------------
# DAG parameters
# --------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'youtube_etl_star_schema',
    default_args=default_args,
    description='ETL DAG: CSV to DW Star Schema',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=True,
    params={
        "csv_path": "/opt/airflow/dags/youtube_data.csv",
        "run_date": None  # YYYY-MM-DD
    },
    on_failure_callback=notify_failure
)

# --------------------
# SQL для таблиц
# --------------------
create_tables_sql = """
CREATE TABLE IF NOT EXISTS stg_videos (
    user_id INT,
    video_id INT,
    video_duration INT,
    watch_time FLOAT,
    liked INT,
    commented INT,
    subscribed_after INT,
    category VARCHAR(100),
    device VARCHAR(50),
    watch_time_of_day VARCHAR(20),
    recommended INT,
    clicked INT,
    timestamp TIMESTAMP,
    watch_percent FLOAT
);

CREATE TABLE IF NOT EXISTS dim_category (
    id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_device (
    id SERIAL PRIMARY KEY,
    device_name VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_time (
    id SERIAL PRIMARY KEY,
    watch_time_of_day VARCHAR(20) UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_watch (
    id SERIAL PRIMARY KEY,
    user_id INT,
    video_id INT,
    video_duration INT,
    watch_time FLOAT,
    liked INT,
    commented INT,
    subscribed_after INT,
    category_id INT REFERENCES dim_category(id),
    device_id INT REFERENCES dim_device(id),
    time_id INT REFERENCES dim_time(id),
    recommended INT,
    clicked INT,
    timestamp TIMESTAMP,
    watch_percent FLOAT
);
"""

# --------------------
# Task 1: Создание таблиц
# --------------------
create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='postgres_etl_target_conn',
    sql=create_tables_sql,
    dag=dag
)

# --------------------
# Task 1.1: Создание уникального индекса
# --------------------
create_unique_index_stg = PostgresOperator(
    task_id='create_unique_index_stg',
    postgres_conn_id='postgres_etl_target_conn',
    sql="""
    CREATE UNIQUE INDEX IF NOT EXISTS stg_videos_user_video_ts_idx
    ON stg_videos(user_id, video_id, timestamp);
    """,
    dag=dag
)

# --------------------
# Task 0: Очистка таблиц
# --------------------
def truncate_tables(pg_conn_id='postgres_etl_target_conn', **kwargs):
    dag_run = kwargs.get('dag_run')
    params = kwargs.get('params', {})
    
    run_date = None
    if dag_run and dag_run.conf:
        run_date = dag_run.conf.get('run_date')
    else:
        run_date = params.get('run_date')
    
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)

    try:
        if run_date:
            logger.info(f"Удаляем данные за дату {run_date}")
            pg_hook.run("DELETE FROM stg_videos WHERE DATE(timestamp) = %s", parameters=[run_date])
            pg_hook.run("""
                DELETE FROM fact_watch
                WHERE DATE(timestamp) = %s
            """, parameters=[run_date])
        else:
            logger.info("Полная очистка staging и fact_watch")
            pg_hook.run("TRUNCATE TABLE stg_videos CASCADE;")
            pg_hook.run("TRUNCATE TABLE fact_watch CASCADE;")
        logger.info("Очистка завершена")
    except Exception as e:
        logger.error(f"Ошибка при очистке: {e}")
        raise AirflowFailException(f"Failed to truncate tables: {e}")

truncate_task = PythonOperator(
    task_id='truncate_tables',
    python_callable=truncate_tables,
    provide_context=True,
    dag=dag
)

# --------------------
# Task 2: Загрузка CSV в staging
# --------------------
def load_csv_to_staging(csv_path: str, pg_conn_id: str, **kwargs):
    dag_run = kwargs.get('dag_run')
    params = kwargs.get('params', {})
    
    run_date = None
    if dag_run and dag_run.conf:
        run_date = dag_run.conf.get('run_date')
    else:
        run_date = params.get('run_date')
    
    try:
        df = pd.read_csv(csv_path)

        for col in ['liked', 'commented', 'subscribed_after']:
            df[col] = df[col].map({'yes': 1, 'no': 0}).fillna(0).astype(int)

        for col in ['watch_time', 'video_duration', 'watch_percent']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        try:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', unit='s')
        except:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', infer_datetime_format=True)

        df = df.dropna(subset=['timestamp', 'watch_time', 'video_duration', 'watch_percent'])

        if run_date:
            df = df[df['timestamp'].dt.date == pd.to_datetime(run_date).date()]
            logger.info(f"Фильтрация по run_date={run_date}, строк: {len(df)}")

        pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)

        for _, row in df.iterrows():
            sql = """
                INSERT INTO stg_videos
                (user_id, video_id, video_duration, watch_time, liked, commented, subscribed_after,
                 category, device, watch_time_of_day, recommended, clicked, timestamp, watch_percent)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, video_id, timestamp) DO NOTHING
            """
            params_row = (
                row['user_id'], row['video_id'], row['video_duration'], row['watch_time'],
                row['liked'], row['commented'], row['subscribed_after'], row['category'],
                row['device'], row['watch_time_of_day'], row['recommended'], row['clicked'],
                row['timestamp'].to_pydatetime(), row['watch_percent']
            )
            pg_hook.run(sql, parameters=params_row)

        logger.info(f"CSV успешно загружен в staging, строк: {len(df)}")
    except Exception as e:
        logger.error(f"Ошибка при загрузке CSV: {e}")
        raise AirflowFailException(f"Failed to load CSV to staging: {e}")

load_csv_task = PythonOperator(
    task_id='load_csv_to_staging',
    python_callable=load_csv_to_staging,
    op_kwargs={'csv_path': "/opt/airflow/dags/youtube_data.csv",
               'pg_conn_id': 'postgres_etl_target_conn'},
    provide_context=True,
    dag=dag
)

# --------------------
# Task 3: Трансформация в star schema
# --------------------
def transform_to_star_schema(**kwargs):
    dag_run = kwargs.get('dag_run')
    params = kwargs.get('params', {})
    run_date = None
    if dag_run and dag_run.conf:
        run_date = dag_run.conf.get('run_date')
    else:
        run_date = params.get('run_date')

    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        # dim tables
        pg_hook.run("""
            INSERT INTO dim_category (category_name)
            SELECT DISTINCT category
            FROM stg_videos
            ON CONFLICT (category_name) DO NOTHING
        """)
        pg_hook.run("""
            INSERT INTO dim_device (device_name)
            SELECT DISTINCT device
            FROM stg_videos
            ON CONFLICT (device_name) DO NOTHING
        """)
        pg_hook.run("""
            INSERT INTO dim_time (watch_time_of_day)
            SELECT DISTINCT watch_time_of_day
            FROM stg_videos
            ON CONFLICT (watch_time_of_day) DO NOTHING
        """)

        # Удаляем fact за run_date для корректного re-fill
        if run_date:
            logger.info(f"Удаляем fact_watch за дату {run_date}")
            pg_hook.run("""
                DELETE FROM fact_watch
                WHERE DATE(timestamp) = %s
            """, parameters=[run_date])

        # fact_watch
        sql_insert_fact = """
            INSERT INTO fact_watch (
                user_id, video_id, video_duration, watch_time, liked, commented,
                subscribed_after, category_id, device_id, time_id,
                recommended, clicked, timestamp, watch_percent
            )
            SELECT
                s.user_id, s.video_id, s.video_duration, s.watch_time, s.liked, s.commented,
                s.subscribed_after, c.id, d.id, t.id,
                s.recommended, s.clicked, s.timestamp, s.watch_percent
            FROM stg_videos s
            JOIN dim_category c ON s.category = c.category_name
            JOIN dim_device d ON s.device = d.device_name
            JOIN dim_time t ON s.watch_time_of_day = t.watch_time_of_day
        """
        if run_date:
            sql_insert_fact += " WHERE DATE(s.timestamp) = '%s'" % run_date

        pg_hook.run(sql_insert_fact)
        logger.info("Трансформация в star schema выполнена успешно")
    except Exception as e:
        logger.error(f"Ошибка при трансформации: {e}")
        raise AirflowFailException(f"Failed to transform to star schema: {e}")

transform_task = PythonOperator(
    task_id='transform_to_star_schema',
    python_callable=transform_to_star_schema,
    provide_context=True,
    dag=dag
)

# --------------------
# Task 4: Telegram уведомление
# --------------------
def send_telegram_message(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": message}
    try:
        r = requests.post(url, data=payload)
        r.raise_for_status()
        logging.info(f"Telegram message sent: {message}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send telegram message: {e}")

notify = PythonOperator(
    task_id="send_telegram_alert",
    python_callable=send_telegram_message,
    op_args=["DAG 'youtube_etl_star_schema' успешно завершился!"],
    dag=dag
)

# --------------------
# Последовательность тасков
# --------------------
truncate_task >> create_tables >> create_unique_index_stg >> load_csv_task >> transform_task >> notify
