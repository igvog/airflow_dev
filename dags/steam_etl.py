from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import requests
import json
from airflow.utils.helpers import chain


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'steam_etl',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval=timedelta(days=1),  
    start_date=datetime(2025, 1, 1),
    catchup= True,
    tags=['etl', 'api', 'datawarehouse', 'star-schema'],
) as dag:
    # ===============RE-FILL======================
    def truncate_staging(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        tables_to_truncate = [
            'stg_applications',
            'stg_reviews',
            'stg_categories',
            'stg_genres',
            'stg_developers',
            'stg_publishers',
            'stg_platforms',
            'stg_application_categories',
            'stg_application_genres',
            'stg_application_developers',
            'stg_application_publishers',
            'stg_application_platforms'
        ]
        for table in tables_to_truncate:
            hook.run(f"TRUNCATE TABLE {table} CASCADE")
        logging.info("Staging tables truncated successfully")

    truncate_staging_task = PythonOperator(
       task_id='truncate_staging',
       python_callable=truncate_staging
    )
    # ===============STAGING LAYER================
    
    def create_staging_tables(*context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        drop_staging = """
        DROP TABLE IF EXISTS stg_applications CASCADE;
        DROP TABLE IF EXISTS stg_reviews CASCADE;
        DROP TABLE IF EXISTS stg_application_categories CASCADE;
        DROP TABLE IF EXISTS stg_application_genres CASCADE;
        DROP TABLE IF EXISTS stg_application_developers CASCADE;
        DROP TABLE IF EXISTS stg_application_publishers CASCADE;
        DROP TABLE IF EXISTS stg_application_platforms CASCADE;
        DROP TABLE IF EXISTS stg_categories CASCADE;
        DROP TABLE IF EXISTS stg_genres CASCADE;
        DROP TABLE IF EXISTS stg_developers CASCADE;
        DROP TABLE IF EXISTS stg_publishers CASCADE;
        DROP TABLE IF EXISTS stg_platforms CASCADE;
        """
        

        create_stage_query = """
            CREATE TABLE stg_applications (
                appid BIGINT PRIMARY KEY,
                name TEXT,
                type TEXT,
                is_free BOOLEAN,
                release_date DATE,
                required_age TEXT,
                short_description TEXT,
                supported_languages TEXT,
                header_image TEXT,
                background TEXT,
                metacritic_score FLOAT,
                recommendations_total FLOAT,
                mat_supports_windows BOOLEAN,
                mat_supports_mac BOOLEAN,
                mat_supports_linux BOOLEAN,
                mat_initial_price FLOAT,
                mat_final_price FLOAT,
                mat_discount_percent FLOAT,
                mat_currency TEXT,
                mat_achievement_count FLOAT,
                mat_pc_os_min TEXT,
                mat_pc_processor_min TEXT,
                mat_pc_memory_min TEXT,
                mat_pc_graphics_min TEXT,
                mat_pc_os_rec TEXT,
                mat_pc_processor_rec TEXT,
                mat_pc_memory_rec TEXT,
                mat_pc_graphics_rec TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
            CREATE TABLE stg_reviews (
                recommendationid BIGINT PRIMARY KEY,
                appid BIGINT,
                author_steamid BIGINT,
                author_num_games_owned INT,
                author_num_reviews INT,
                author_playtime_forever INT,
                author_playtime_last_two_weeks INT,
                author_playtime_at_review FLOAT,
                author_last_played TIMESTAMP,
                language TEXT,
                review_text TEXT,
                timestamp_created TIMESTAMP,
                timestamp_updated TIMESTAMP,
                voted_up BOOLEAN,
                votes_up INT,
                votes_funny INT,
                weighted_vote_score FLOAT,
                comment_count INT,
                steam_purchase BOOLEAN,
                received_for_free BOOLEAN,
                written_during_early_access BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            );
            CREATE TABLE stg_categories (
                category_id INT PRIMARY KEY,
                category_description TEXT
            );
            CREATE TABLE stg_genres (
                genre_id INT PRIMARY KEY,
                genre_description TEXT
            );
            CREATE TABLE stg_developers (
                developer_id INT PRIMARY KEY,
                developer_name TEXT
            );
            CREATE TABLE stg_publishers (
                publisher_id INT PRIMARY KEY,
                publisher_name TEXT
            );
            CREATE TABLE stg_platforms (
                platform_id INT PRIMARY KEY,
                platform_name TEXT
            );
            CREATE TABLE stg_application_categories (
                appid BIGINT,
                category_id INT
            );
            CREATE TABLE stg_application_genres (
                appid BIGINT,
                genre_id INT
            );
            CREATE TABLE stg_application_developers (
                appid BIGINT,
                developer_id INT
            );
            CREATE TABLE stg_application_publishers (
                appid BIGINT,
                publisher_id INT
            );
            CREATE TABLE stg_application_platforms (
                appid BIGINT,
                platform_id INT
            );
            """
        hook.run(drop_staging)
        hook.run(create_stage_query)
        logging.info("Staging tables created successfully")

    create_staging = PythonOperator(
        task_id="create_staging_tables",
        python_callable = create_staging_tables
    )
    
    def load_csv_to_staging(file_path, table_name, pk=None):
        pg_hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
    
        try:
            df = pd.read_csv(file_path, nrows = 250000)
        
            rename_map = {
                "stg_categories": {"id": "category_id", "name": "category_description"},
                "stg_genres": {"id": "genre_id", "name": "genre_description"},
                "stg_developers": {"id": "developer_id", "name": "developer_name"},
                "stg_publishers": {"id": "publisher_id", "name": "publisher_name"},
                "stg_platforms": {"id": "platform_id", "name": "platform_name"},
            }
            if table_name in rename_map:
                df = df.rename(columns=rename_map[table_name])
        
            logging.info(f"Loaded {len(df)} rows from {file_path}")
        

            timestamp_cols = ["author_last_played", "timestamp_created", "timestamp_updated", "created_at", "updated_at"]
            for col in timestamp_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], unit="s", errors="coerce")

        

            if pk and pk in df.columns:
                df = df.drop_duplicates(subset=[pk])
        

            for _, row in df.iterrows():
                cols = ','.join(row.index)
                vals_list = []
                for v in row.values:
                    if pd.isnull(v):
                        vals_list.append('NULL')
                    elif isinstance(v, pd.Timestamp):
                        vals_list.append(f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                    elif isinstance(v, (int, float)):
                        vals_list.append(str(v))
                    else:
                        vals_list.append(f"'{str(v).replace('\'','\'\'')}'")
                vals = ','.join(vals_list)
                sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
                cursor.execute(sql)
        
            conn.commit()
            logging.info(f"{table_name} loaded successfully.")
        except Exception as e:
            logging.error(f"Error loading {table_name}: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

        
            
    load_applications_stage = PythonOperator(
        task_id='load_applications_stage',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'file_path': '/steam_dataset_2025_csv/applications.csv',
            'table_name': 'stg_applications',
            'pk': 'appid'
        }
    )


    load_reviews_stage = PythonOperator(
        task_id='load_reviews_stage',
        python_callable=load_csv_to_staging,
        op_kwargs={
            'file_path': '/steam_dataset_2025_csv/reviews.csv',
            'table_name': 'stg_reviews',
            'pk': 'recommendationid'
        }
    )


    def load_stg_others_func():
        files_tables = [ {"file": "/steam_dataset_2025_csv/categories.csv", "table": "stg_categories", "pk": "category_id"}, 
                     {"file": "/steam_dataset_2025_csv/genres.csv", "table": "stg_genres", "pk": "genre_id"}, 
                     {"file": "/steam_dataset_2025_csv/developers.csv", "table": "stg_developers", "pk": "developer_id"}, 
                     {"file": "/steam_dataset_2025_csv/publishers.csv", "table": "stg_publishers", "pk": "publisher_id"}, 
                     {"file": "/steam_dataset_2025_csv/platforms.csv", "table": "stg_platforms", "pk": "platform_id"}, 
                     {"file": "/steam_dataset_2025_csv/application_categories.csv", "table": "stg_application_categories"}, 
                     {"file": "/steam_dataset_2025_csv/application_genres.csv", "table": "stg_application_genres"}, 
                     {"file": "/steam_dataset_2025_csv/application_developers.csv", "table": "stg_application_developers"}, 
                     {"file": "/steam_dataset_2025_csv/application_publishers.csv", "table": "stg_application_publishers"}, 
                     {"file": "/steam_dataset_2025_csv/application_platforms.csv", "table": "stg_application_platforms"}, ]
        for cfg in files_tables:
            load_csv_to_staging(cfg["file"], cfg["table"])

    load_stg_others = PythonOperator(
        task_id='load_stg_others',
        python_callable=load_stg_others_func
    )
    
    
    # ========== STAR  SCHEMA ==========
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS dim_applications (
            app_key SERIAL PRIMARY KEY,
            appid BIGINT UNIQUE NOT NULL,
            name TEXT,
            type TEXT,
            is_free BOOLEAN,
            release_date DATE,
            supports_windows BOOLEAN DEFAULT FALSE,
            supports_mac BOOLEAN DEFAULT FALSE,
            supports_linux BOOLEAN DEFAULT FALSE,
            final_price FLOAT DEFAULT 0,
            currency TEXT,
            discount_percent FLOAT DEFAULT 0,
            achievement_count INT DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS dim_users (
            user_key SERIAL PRIMARY KEY,
            steamid BIGINT UNIQUE NOT NULL,
            num_games_owned INT DEFAULT 0,
            num_reviews INT DEFAULT 0,
            playtime_forever INT DEFAULT 0,
            playtime_last_two_weeks INT DEFAULT 0,
            last_played TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS dim_dates (
            date_key INT PRIMARY KEY,
            full_date DATE NOT NULL,
            year INT,
            month INT,
            day INT,
            quarter INT,
            day_of_week INT,
            month_name TEXT,
            day_name TEXT,
            is_weekend BOOLEAN
            );
        CREATE TABLE IF NOT EXISTS fact_game_prices (
            fact_price_id SERIAL PRIMARY KEY,
            application_id BIGINT NOT NULL,
            game_name TEXT,
            date_key INTEGER NOT NULL,
            initial_price FLOAT DEFAULT 0,
            final_price FLOAT DEFAULT 0,
            discount_percent FLOAT DEFAULT 0,
            is_free BOOLEAN DEFAULT FALSE
        );
        CREATE TABLE IF NOT EXISTS fact_game_engagement (
            fact_id SERIAL PRIMARY KEY,
            application_id BIGINT NOT NULL,
            game_name TEXT,
            type TEXT,
            is_free BOOLEAN,
            platform_windows BOOLEAN,
            platform_mac BOOLEAN,
            platform_linux BOOLEAN,
            release_year INT,
            recommendations_total INT DEFAULT 0,
            metacritic_score FLOAT DEFAULT 0,
            achievement_count INT DEFAULT 0,
            final_price FLOAT DEFAULT 0,
            discount_percent FLOAT DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS fact_reviews (
            fact_reviews_id SERIAL PRIMARY KEY,
            application_id BIGINT NOT NULL,
            reviewer_id BIGINT,
            date_key INTEGER NOT NULL,
            voted_up BOOLEAN DEFAULT FALSE,
            votes_up INT DEFAULT 0,
            votes_funny INT DEFAULT 0,
            weighted_vote_score FLOAT DEFAULT 0,
            playtime_forever INT DEFAULT 0,
            playtime_last_two_weeks INT DEFAULT 0
        );

        """
    )

    def populate_dim_applications(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        query = """
        INSERT INTO dim_applications (
            appid,
            name,
            type,
            is_free,
            release_date,
            supports_windows,
            supports_mac,
            supports_linux,
            final_price,
            currency,
            discount_percent,
            achievement_count,
            updated_at
        )
        SELECT
            appid,
            name,
            type,
            is_free,
            release_date,
            COALESCE(mat_supports_windows, FALSE),
            COALESCE(mat_supports_mac, FALSE),
            COALESCE(mat_supports_linux, FALSE),
            CASE 
                WHEN is_free THEN 0
                ELSE COALESCE(mat_final_price, 0)
            END AS final_price,
            COALESCE(mat_currency, 'USD'),
            COALESCE(mat_discount_percent, 0),
            COALESCE(mat_achievement_count, 0),
            NOW()
        FROM stg_applications
        WHERE appid IS NOT NULL
        ON CONFLICT (appid) DO UPDATE SET
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            is_free = EXCLUDED.is_free,
            release_date = EXCLUDED.release_date,
            supports_windows = EXCLUDED.supports_windows,
            supports_mac = EXCLUDED.supports_mac,
            supports_linux = EXCLUDED.supports_linux,
            final_price = EXCLUDED.final_price,
            currency = EXCLUDED.currency,
            discount_percent = EXCLUDED.discount_percent,
            achievement_count = EXCLUDED.achievement_count,
            updated_at = NOW();
        """
        hook.run(query)
        logging.info("Populated dim_applications dimension table")


    def populate_dim_user(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn') 

        query = """
        INSERT INTO dim_users (
            steamid,
            num_games_owned,
            num_reviews,
            playtime_forever,
            playtime_last_two_weeks,
            last_played,
            updated_at
        )
        SELECT DISTINCT ON (author_steamid)
            author_steamid,
            COALESCE(author_num_games_owned, 0),
            COALESCE(author_num_reviews, 0),
            COALESCE(author_playtime_forever, 0),
            COALESCE(author_playtime_last_two_weeks, 0),
            author_last_played,
            NOW()
        FROM stg_reviews
        WHERE author_steamid IS NOT NULL
        ORDER BY author_steamid, timestamp_created DESC
        ON CONFLICT (steamid) DO UPDATE SET
            num_games_owned = EXCLUDED.num_games_owned,
            num_reviews = EXCLUDED.num_reviews,
            playtime_forever = EXCLUDED.playtime_forever,
            playtime_last_two_weeks = EXCLUDED.playtime_last_two_weeks,
            last_played = EXCLUDED.last_played,
            updated_at = NOW();

        """

        hook.run(query)
        logging.info("Populated dim_users dimension table")


    def populate_dim_dates(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn') 

        query = """
        INSERT INTO dim_dates (
            date_key,
            full_date,
            year,
            month,
            day,
            quarter,
            day_of_week,
            month_name,
            day_name,
            is_weekend
        )
        SELECT DISTINCT
            TO_CHAR(timestamp_created::date, 'YYYYMMDD')::INT AS date_key,
            timestamp_created::date AS full_date,
            EXTRACT(YEAR FROM timestamp_created),
            EXTRACT(MONTH FROM timestamp_created),
            EXTRACT(DAY FROM timestamp_created),
            EXTRACT(QUARTER FROM timestamp_created),
            EXTRACT(DOW FROM timestamp_created),
            TO_CHAR(timestamp_created::date, 'Month'),
            TO_CHAR(timestamp_created::date, 'Day'),
            CASE WHEN EXTRACT(DOW FROM timestamp_created) IN (0, 6) THEN TRUE ELSE FALSE END
        FROM stg_reviews
        WHERE timestamp_created IS NOT NULL
        ON CONFLICT (date_key) DO NOTHING;
        """

        hook.run(query)
        logging.info("Populated dim_users dimension table")
        
    def populate_fact_reviews(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn') 
        query = """
        INSERT INTO fact_reviews (application_id, reviewer_id, date_key, voted_up, votes_up, votes_funny, weighted_vote_score, playtime_forever, playtime_last_two_weeks)
        SELECT
            appid,
            author_steamid,
            TO_CHAR(COALESCE(timestamp_created, CURRENT_DATE), 'YYYYMMDD')::INTEGER AS date_key,
            COALESCE(voted_up, FALSE),
            COALESCE(votes_up, 0),
            COALESCE(votes_funny, 0),
            COALESCE(weighted_vote_score, 0),
            COALESCE(author_playtime_forever, 0),
            COALESCE(author_playtime_last_two_weeks, 0)
        FROM stg_reviews;
        """
        hook.run(query)
        logging.info("Populated fact_reviews fact table")

    def populate_fact_game_prices(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn') 
        query = """
        INSERT INTO fact_game_prices (application_id, game_name,date_key, initial_price, final_price, discount_percent, is_free)
        SELECT
            appid,
            name as game_name,
            TO_CHAR(COALESCE(release_date, CURRENT_DATE), 'YYYYMMDD')::INTEGER AS date_key,
            COALESCE(mat_initial_price, 0) AS initial_price,
            COALESCE(mat_final_price, 0) AS final_price,
            COALESCE(mat_discount_percent, 0) AS discount_percent,
            COALESCE(is_free, FALSE) AS is_free
        FROM stg_applications;        
        """
        hook.run(query)
        logging.info("Populated fact_game_prices fact table")

    def populate_fact_game_engagement(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn') 
        query = """
        INSERT INTO fact_game_engagement (
            application_id, game_name, type, is_free,
            platform_windows, platform_mac, platform_linux,
            release_year, recommendations_total, metacritic_score,
            achievement_count, final_price, discount_percent
        )
        SELECT
            appid,
            name AS game_name,
            type,
            is_free,
            mat_supports_windows,
            mat_supports_mac,
            mat_supports_linux,
            EXTRACT(YEAR FROM release_date)::INT,
            COALESCE(recommendations_total, 0),
            COALESCE(metacritic_score, 0),
            COALESCE(mat_achievement_count, 0),
            COALESCE(mat_final_price, 0),
            COALESCE(mat_discount_percent, 0)
        FROM stg_applications;
        """
        hook.run(query)
        logging.info("Populated populate_fact_game_engagement fact table")
        
        
    load_dim_applications = PythonOperator(
        task_id='populate_dim_applications',
        python_callable=populate_dim_applications
    )

    load_dim_users = PythonOperator(
        task_id='populate_dim_users',
        python_callable=populate_dim_user
    )

    load_dim_dates = PythonOperator(
        task_id='populate_dim_dates',
        python_callable=populate_dim_dates
    )
    
    load_fact_reviews = PythonOperator(
        task_id='populate_fact_reviews',
        python_callable=populate_fact_reviews
    )

    load_fact_game_prices = PythonOperator(
        task_id='populate_fact_game_prices',
        python_callable=populate_fact_game_prices
    )

    load_fact_game_engagement = PythonOperator(
        task_id='populate_fact_game_engagement',
        python_callable=populate_fact_game_engagement
    )
    create_staging >> truncate_staging_task >> [load_applications_stage, load_reviews_stage, load_stg_others] >> create_dw_schema
    create_dw_schema >> [load_dim_applications, load_dim_users, load_dim_dates]
    chain(
        [load_dim_applications, load_dim_users, load_dim_dates],
        [load_fact_reviews, load_fact_game_prices, load_fact_game_engagement]
    )   