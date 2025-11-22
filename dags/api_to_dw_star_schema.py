"""
ETL DAG: API to Data Warehouse Star Schema
This DAG extracts data from JSONPlaceholder API, loads it into Postgres,
and transforms it into a star schema data warehouse model.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import requests
import json
import logging
import pandas as pd
import numpy as np

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# DAG definition
with DAG(
    'api_to_dw_star_schema',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'api', 'datawarehouse', 'star-schema'],
) as dag:

    # ========== STAGING LAYER ==========
    
    def create_staging_tables(**context):
        """Create staging tables for raw API data"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        # Drop existing staging tables
        drop_staging = """
        DROP TABLE IF EXISTS staging_details CASCADE;
        DROP TABLE IF EXISTS staging_person_details CASCADE;
        DROP TABLE IF EXISTS staging_ratings CASCADE;
        """
        
        # Create staging tables
        create_staging_tables_sql = """
        CREATE TABLE IF NOT EXISTS staging_person_details (
            person_mal_id INT PRIMARY KEY,
            name TEXT,
            given_name TEXT,
            family_name TEXT,
            birthday DATE,
            favorites INT,
            about TEXT
        );

        CREATE TABLE IF NOT EXISTS staging_details (
            mal_id INT PRIMARY KEY,
            title TEXT,
            title_japanese TEXT,
            type TEXT,
            status TEXT,
            score NUMERIC,
            scored_by INT,
            start_date DATE,
            end_date DATE,
            genres TEXT,
            studios TEXT
        ); 

        CREATE TABLE IF NOT EXISTS staging_ratings (
            username TEXT,
            anime_id INT,
            score NUMERIC,
            status TEXT,
            num_watched_episodes INT
        );
        """


        
        hook.run(drop_staging)
        hook.run(create_staging_tables_sql)
        logging.info("Staging tables created successfully")
    
    create_staging = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
    )
    
    def load_staging_person_details(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        # Чтение CSV
        df = pd.read_csv('/opt/airflow/data/raw/datasets/person_details.csv')

        # Преобразование даты
        df['birthday'] = pd.to_datetime(df['birthday'], errors='coerce')
        # Заменяем NaN на None для Postgres
        df = df.replace({pd.NaT: None, np.nan: None})

        # Вставка в Postgres
        for _, row in df.iterrows():
            sql = """
            INSERT INTO staging_person_details (
                person_mal_id, name, given_name, family_name, birthday, favorites, about
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (person_mal_id) DO NOTHING;
            """
            hook.run(sql, parameters=(
                row['person_mal_id'],
                row['name'],
                row['given_name'],
                row['family_name'],
                row['birthday'],
                row['favorites'],
                row['about']
            ))

        logging.info(f"{len(df)} rows inserted into staging_person_details")

    load_staging_person_details = PythonOperator(
        task_id='load_staging_person_details',
        python_callable=load_staging_person_details,
    )
    
    def load_staging_details(**context):
        from psycopg2.extras import execute_values
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        df = pd.read_csv('/opt/airflow/data/raw/datasets/details.csv')

        # Преобразование дат
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

        # Преобразование списков в строки (genres, studios)
        df['genres'] = df['genres'].apply(lambda x: ';'.join(eval(x)) if pd.notnull(x) else None)
        df['studios'] = df['studios'].apply(lambda x: ';'.join(eval(x)) if pd.notnull(x) else None)

        df = df.replace({pd.NaT: None, np.nan: None})

        df['start_date'] = df['start_date'].apply(
            lambda x: None if pd.isna(x) else x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else None
        )
        df['end_date'] = df['end_date'].apply(
            lambda x: None if pd.isna(x) else x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else None
        )
        required_columns = [
            'mal_id', 'title', 'title_japanese', 'type', 'status',
            'score', 'scored_by', 'start_date', 'end_date', 'genres', 'studios'
        ]
        # Проверяем, что все колонки есть
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logging.error(f"Missing columns: {missing}")
            raise ValueError(f"Missing columns: {missing}")
    
        # Выбираем только нужные колонки
        df_insert = df[required_columns]
    
        logging.info(f"Columns to insert: {df_insert.columns.tolist()}")
        logging.info(f"First row: {df_insert.iloc[0].to_dict()}")
        # Batch insert через execute_values (намного быстрее)
        conn = hook.get_conn()
        cursor = conn.cursor()
    
        sql = """
        INSERT INTO staging_details (
            mal_id, title, title_japanese, type, status, score, scored_by,
            start_date, end_date, genres, studios
        ) VALUES %s
        ON CONFLICT (mal_id) DO NOTHING
        """

        # Подготовка данных — используем только выбранные колонки `df_insert`
        values = [tuple(row) for row in df_insert.values]
    
        execute_values(cursor, sql, values, page_size=1000)
        conn.commit()
        cursor.close()

        logging.info(f"{len(df)} rows inserted into staging_details")
    
            
    load_staging_details = PythonOperator(
        task_id='load_staging_details',
        python_callable=load_staging_details,
    )
    
    def load_staging_ratings(**context):
        from psycopg2.extras import execute_values
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        # Читаем только первые 20000 строк (БЕЗ chunksize и цикла)
        df = pd.read_csv('/opt/airflow/data/raw/datasets/ratings.csv', nrows=20000)
    
        # ОТЛАДКА: посмотрим все колонки
        logging.info(f"CSV columns: {df.columns.tolist()}")
        logging.info(f"Total columns: {len(df.columns)}")
    
        # Преобразуем числовые колонки
        if 'score' in df.columns:
            df['score'] = pd.to_numeric(df['score'], errors='coerce')
        if 'num_watched_episodes' in df.columns:
            df['num_watched_episodes'] = pd.to_numeric(df['num_watched_episodes'], errors='coerce')
    
        # Заменяем NaN на None
        df = df.replace({np.nan: None})
    
        # ВАЖНО: Выбираем только нужные 5 колонок
        required_columns = ['username', 'anime_id', 'score', 'status', 'num_watched_episodes']
    
        # Проверяем наличие колонок
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logging.error(f"Missing columns: {missing}")
            logging.error(f"Available columns: {df.columns.tolist()}")
            raise ValueError(f"Missing columns: {missing}")
    
        # Выбираем только нужные колонки
        df_insert = df[required_columns]
    
        logging.info(f"Columns to insert: {df_insert.columns.tolist()}")
        logging.info(f"First row: {df_insert.iloc[0].to_dict()}")
    
        # Batch insert
        conn = hook.get_conn()
        cursor = conn.cursor()
    
        sql = """
        INSERT INTO staging_ratings (username, anime_id, score, status, num_watched_episodes)
        VALUES %s
        """
    
        # Подготовка данных из df_insert (не df!)
        values = [tuple(row) for row in df_insert.values]
    
        execute_values(cursor, sql, values, page_size=1000)
        conn.commit()
        cursor.close()
    
        logging.info(f"{len(df_insert)} rows inserted into staging_ratings")
    
    load_staging_ratings = PythonOperator(
        task_id='load_staging_ratings',
        python_callable=load_staging_ratings,
    )
    
    
    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========
    
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        -- Drop existing DW tables
        DROP TABLE IF EXISTS dim_person  CASCADE;
        DROP TABLE IF EXISTS dim_anime  CASCADE;
        DROP TABLE IF EXISTS fact_ratings  CASCADE;
        
        -- Dimension: Person
        CREATE TABLE IF NOT EXISTS dim_person (
            person_id INT PRIMARY KEY,
            name TEXT,
            given_name TEXT,
            family_name TEXT,
            birthday DATE,
            favorites INT
        );
        
        -- Dimension: Anime
        CREATE TABLE IF NOT EXISTS dim_anime (
            anime_id INT PRIMARY KEY,
            title TEXT,
            title_japanese TEXT,
            type TEXT,
            status TEXT,
            score NUMERIC,
            scored_by INT,
            start_date DATE,
            end_date DATE,
            genres TEXT,
            studios TEXT
        );
        
        -- Fact: Ratings (with metrics)
        CREATE TABLE IF NOT EXISTS fact_ratings (
            rating_id SERIAL PRIMARY KEY,
            username TEXT,
            anime_id INT REFERENCES dim_anime(anime_id),
            score NUMERIC,
            status TEXT,
            num_watched_episodes INT
        );
        """,
    )
    
    def transform_dim_person(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO dim_person (person_id, name, given_name, family_name, birthday, favorites)
        SELECT 
            person_mal_id,
            name,
            given_name,
            family_name,
            birthday,
            favorites
        FROM staging_person_details
        ON CONFLICT (person_id) DO NOTHING;
        """

        hook.run(sql)
        logging.info("dim_person table transformed successfully")

 
    transform_dim_person_task = PythonOperator(
        task_id='transform_dim_person',
        python_callable=transform_dim_person,
    )
    
    def transform_dim_anime(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO dim_anime (anime_id, title, title_japanese, type, status, score, scored_by,
                            start_date, end_date, genres, studios)
        SELECT 
            mal_id,
            title,
            title_japanese,
            type,
            status,
            score,
            scored_by,
            start_date,
            end_date,
            genres,
            studios
        FROM staging_details
        ON CONFLICT (anime_id) DO NOTHING;
        """

        hook.run(sql)
        logging.info("dim_anime table transformed successfully")

    transform_dim_anime = PythonOperator(
        task_id='transform_dim_anime',
        python_callable=transform_dim_anime,
    )
    
    def transform_fact_ratings(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        sql = """
        INSERT INTO fact_ratings (username, anime_id, score, status, num_watched_episodes)
        SELECT 
            r.username,
            r.anime_id,
            r.score,
            r.status,
            r.num_watched_episodes
        FROM staging_ratings r
        JOIN dim_anime a ON r.anime_id = a.anime_id;
        """

        hook.run(sql)
        logging.info("fact_ratings table transformed successfully")

    transform_fact_ratings = PythonOperator(
        task_id='transform_fact_ratings',
        python_callable=transform_fact_ratings,
    )
    
    # ========== TASK DEPENDENCIES ==========
    
    # Staging layer
    create_staging >> [load_staging_person_details, load_staging_details, load_staging_ratings]
    
    # Data warehouse layer
    [load_staging_person_details, load_staging_details, load_staging_ratings] >> create_dw_schema
    create_dw_schema >> [transform_dim_person_task, transform_dim_anime]
    [transform_dim_person_task, transform_dim_anime] >> transform_fact_ratings

