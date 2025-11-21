from datetime import datetime, timedelta
import logging
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import numpy as np
from airflow.exceptions import AirflowSkipException
import json 


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

POSTGRES_CONN_ID = 'postgres_etl_target_conn'

MOVIES_FILE = '/opt/airflow/dags/files/movies_metadata.csv/movies_metadata.csv'
RATINGS_FILE = '/opt/airflow/dags/files/ratings.csv/ratings.csv'

default_args = {
    'owner': 'Shugyla',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}
with DAG(
    dag_id='movie_ratings_star_schema_etl',
    default_args=default_args,
    description='ETL DAG для загрузки фильмов и рейтингов в Star Schema',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'movies', 'star-schema', 'correct'],
) as dag:
    def get_run_date(**kwargs):
        run_date = kwargs['ds']
        logging.info(f"Run date: {run_date}")
        return run_date
    get_run_date_task = PythonOperator(
        task_id='get_run_date',
        python_callable=get_run_date,
        provide_context=True,
    )
    

    def truncate_staging_for_run_date(**kwargs):
        run_date = kwargs['ti'].xcom_pull(task_ids='get_run_date')
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
    
        # удаляем данные для run_date из staging
        cur.execute("DELETE FROM staging_ratings WHERE DATE(rating_timestamp) = %s", (run_date,))
        cur.execute("DELETE FROM staging_metadata WHERE DATE(release_date) = %s", (run_date,))
    
        conn.commit()
        cur.close()
        logging.info(f"Старые данные для {run_date} удалены из staging.")


    truncate_staging_for_run_date = PythonOperator(
        task_id='truncate_staging_for_run_date',
        python_callable=truncate_staging_for_run_date,
        provide_context=True,
    )


    def create_all_tables(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
        sql = """
        -- Staging
        DROP TABLE IF EXISTS staging_metadata CASCADE;
        DROP TABLE IF EXISTS staging_ratings CASCADE;
    
        CREATE TABLE staging_metadata (
            movie_id INTEGER PRIMARY KEY,
            title TEXT,
            genres TEXT,
            release_date DATE,
            budget NUMERIC,
            revenue NUMERIC,
            tagline TEXT,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    
        CREATE TABLE staging_ratings (
            user_id INTEGER,
            movie_id INTEGER,
            rating NUMERIC(2,1),
            rating_timestamp TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    
        -- DW tables
        DROP TABLE IF EXISTS dim_users CASCADE;
        DROP TABLE IF EXISTS dim_movies CASCADE;
        DROP TABLE IF EXISTS fact_ratings CASCADE;

        CREATE TABLE dim_users (
            user_key SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL
       );

        CREATE TABLE dim_movies (
            movie_key SERIAL PRIMARY KEY,
            movie_id INTEGER UNIQUE NOT NULL,
            title TEXT,
            release_date DATE,
            budget NUMERIC,
            revenue NUMERIC,
            tagline TEXT,
            main_genre VARCHAR(100),
            all_genres_list TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE fact_ratings (
            rating_key BIGSERIAL PRIMARY KEY,
            movie_key INTEGER REFERENCES dim_movies(movie_key),
            user_key INTEGER REFERENCES dim_users(user_key),
            date_key INTEGER,
            rating NUMERIC(2,1),
            rating_timestamp TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        -- DW tables
            DROP TABLE IF EXISTS dim_date CASCADE;

        CREATE TABLE dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            weekday INTEGER
        );

        CREATE INDEX idx_fact_ratings_movie_key ON fact_ratings(movie_key);
        CREATE INDEX idx_fact_ratings_user_key ON fact_ratings(user_key);
        """
        hook.run(sql)
        logging.info("Все таблицы DW и Staging созданы успешно.")

    create_tables_task = PythonOperator(
        task_id='create_all_tables',
        python_callable=create_all_tables,
    )


    def populate_dim_users(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
        sql = """
        INSERT INTO dim_users (user_id)
        SELECT DISTINCT sr.user_id
        FROM staging_ratings sr
        LEFT JOIN dim_users du ON sr.user_id = du.user_id
        WHERE du.user_id IS NULL
        ON CONFLICT (user_id) DO NOTHING;

        """
        hook.run(sql)
        logging.info("dim_users заполнен новыми пользователями.")
    populate_dim_users_task = PythonOperator(
        task_id='populate_dim_users',
        python_callable=populate_dim_users,
    )


    def populate_dim_movies(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
        sql = """
        INSERT INTO dim_movies (movie_id, title, release_date, budget, revenue, tagline, main_genre, all_genres_list)
        SELECT
            sm.movie_id,
            sm.title,
            sm.release_date,
            sm.budget,
            sm.revenue,
            sm.tagline,
            -- main_genre: первый жанр из JSON
            (SELECT (genre->>'name') FROM json_array_elements(sm.genres::json) genre LIMIT 1) AS main_genre,
            -- all_genres_list: строка всех жанров
            (SELECT STRING_AGG(genre->>'name', ', ') FROM json_array_elements(sm.genres::json) genre) AS all_genres_list
        FROM staging_metadata sm
        ON CONFLICT (movie_id) DO UPDATE SET
            title = EXCLUDED.title,
            release_date = EXCLUDED.release_date,
            budget = EXCLUDED.budget,
            revenue = EXCLUDED.revenue,
            tagline = EXCLUDED.tagline,
            main_genre = EXCLUDED.main_genre,
            all_genres_list = EXCLUDED.all_genres_list,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql)
        logging.info("dim_movies заполнен и обновлен.")
    populate_dim_movies_task = PythonOperator(
        task_id='populate_dim_movies',
        python_callable=populate_dim_movies,
    )


    def populate_fact_ratings(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
        sql = """
        INSERT INTO fact_ratings (movie_key, user_key, date_key, rating, rating_timestamp)
        SELECT
            dm.movie_key,
            du.user_key,
            TO_CHAR(sr.rating_timestamp, 'YYYYMMDD')::INTEGER AS date_key,
            sr.rating,
            sr.rating_timestamp
        FROM staging_ratings sr
        JOIN dim_movies dm ON sr.movie_id = dm.movie_id
        JOIN dim_users du ON sr.user_id = du.user_id;
        """
        hook.run(sql)
        logging.info("fact_ratings заполнен.")


    populate_fact_ratings_task = PythonOperator(
        task_id='populate_fact_ratings',
        python_callable=populate_fact_ratings,
    )
    

    def populate_dim_date(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
        WITH min_max_ts AS (
            SELECT 
                DATE(MIN(rating_timestamp)) AS start_date, 
                DATE(MAX(rating_timestamp)) AS end_date
            FROM staging_ratings
        ),
        all_dates AS (
            SELECT 
                generate_series(mmt.start_date, mmt.end_date, '1 day'::interval) AS full_date
            FROM min_max_ts mmt
        )
        INSERT INTO dim_date (date_key, full_date, year, month, day, weekday)
        SELECT
            TO_CHAR(d.full_date, 'YYYYMMDD')::INTEGER AS date_key,
            d.full_date,
            EXTRACT(YEAR FROM d.full_date)::INT AS year,
            EXTRACT(MONTH FROM d.full_date)::INT AS month,
            EXTRACT(DAY FROM d.full_date)::INT AS day,
            EXTRACT(DOW FROM d.full_date)::INT AS weekday -- 0=Воскресенье, 6=Суббота
        FROM all_dates d
        ON CONFLICT (date_key) DO NOTHING;
        """
        hook.run(sql)
        logging.info("dim_date заполнен динамически на основе данных рейтингов.")


    populate_dim_date_task = PythonOperator(
        task_id='populate_dim_date',
        python_callable=populate_dim_date,
    )


    def load_staging_from_local_csv(**context):
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        try:
            logging.info("Загрузка movies_metadata.csv...")

            df = pd.read_csv(MOVIES_FILE, low_memory=False)

            df = df[df['id'].astype(str).str.isnumeric()]
            df['movie_id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
            df = df.drop_duplicates(subset=['movie_id'], keep='first')

            df = df[['movie_id','title','genres','release_date','budget','revenue','tagline']]
            # df['genres'] = df['genres'].astype(str)
            def fix_genres(value):
                if isinstance(value, list):
                    return json.dumps(value)
                if isinstance(value, str):
                    # делим по запятой, убираем пробелы
                    parts = [p.strip() for p in value.split(",")]
                    return json.dumps(parts)
                return json.dumps([])

            df["genres"] = df["genres"].apply(fix_genres)
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
            df['budget'] = pd.to_numeric(df['budget'], errors='coerce')
            df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
            df = df.replace({np.nan: None, pd.NaT: None})
            

            # Временный CSV для COPY
            tmp_path = "/tmp/movies_clean.csv"
            df.to_csv(tmp_path, index=False)

            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute("TRUNCATE TABLE staging_metadata RESTART IDENTITY CASCADE;")
            conn.commit()

            with open(tmp_path, "r", encoding="utf-8") as f:
                cur.copy_expert("""
                COPY staging_metadata (movie_id, title, genres, release_date, budget, revenue, tagline)
                FROM STDIN WITH CSV HEADER;
                """, f)

            conn.commit()
            cur.close()
            logging.info(f"staging_metadata загружена: {len(df)} строк")

        except Exception as e:
            logging.error(f"Ошибка загрузки метаданных: {e}")
            raise AirflowSkipException()

    # Загрузка ratings.csv 
        try:
            logging.info("Загрузка ratings.csv с chunking ...")

            conn=hook.get_conn()
            cur=conn.cursor()
            cur.execute("TRUNCATE TABLE staging_ratings RESTART IDENTITY CASCADE;")
            conn.commit()

            chunk_size = 20000
            for chunk in pd.read_csv(RATINGS_FILE, chunksize=chunk_size):
                chunk = chunk[['userId','movieId','rating','timestamp']]
                chunk = chunk.rename(columns={'userId':'user_id','movieId':'movie_id'})
                chunk['user_id'] = pd.to_numeric(chunk['user_id'], errors='coerce')
                chunk['movie_id'] = pd.to_numeric(chunk['movie_id'], errors='coerce')
                chunk['rating_timestamp'] = pd.to_datetime(chunk['timestamp'], unit='s', errors='coerce')

                chunk = chunk.dropna(subset=['user_id','movie_id','rating','rating_timestamp'])
                chunk = chunk[['user_id','movie_id','rating','rating_timestamp']]

                tmp_path = "/tmp/ratings_chunk.csv"
                chunk.to_csv(tmp_path, index=False)

                with open(tmp_path, "r", encoding="utf-8") as f:
                    cur.copy_expert("""
                    COPY staging_ratings (user_id, movie_id, rating, rating_timestamp)
                    FROM STDIN WITH CSV HEADER;
                    """, f)

                conn.commit()
                logging.info(f"Загружено {len(chunk)} строк в staging_ratings из текущего чанка.")
            

        except Exception as e:
            logging.error(f"Ошибка загрузки рейтингов: {e}")
            raise AirflowSkipException()


    load_staging_task = PythonOperator(
        task_id='load_staging_from_local_csv',
        python_callable=load_staging_from_local_csv,
    )


    get_run_date_task >> truncate_staging_for_run_date >> create_tables_task >> load_staging_task
    load_staging_task >> [populate_dim_users_task, populate_dim_movies_task, populate_dim_date_task]
    [populate_dim_users_task, populate_dim_movies_task, populate_dim_date_task] >> populate_fact_ratings_task




    