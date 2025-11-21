"""
ETL DAG: API to Data Warehouse Star Schema
This DAG extracts data from kaggle using kagglehub, loads it into Postgres,
and transforms it into a star schema data warehouse model.
"""

from datetime import datetime, timedelta
import logging
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kagglehub import dataset_download

from tg_bot import notify_dag_success, notify_task_failure


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'game_info_to_dwh_dag',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_task_failure,
    tags=['etl'],
) as dag:
    
    # ========== STAGING LAYER ==========
    drop_staging_tables = PostgresOperator(
        task_id='drop_staging_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            DROP TABLE IF EXISTS stg_raw_games;
            DROP TABLE IF EXISTS stg_games_clean;
            DROP TABLE IF EXISTS stg_game_platforms;
            DROP TABLE IF EXISTS stg_game_developers;
            DROP TABLE IF EXISTS stg_game_genres;
            DROP TABLE IF EXISTS stg_game_publishers;
        """
    )

    create_raw_games = PostgresOperator(
        task_id='create_staging_raw_games',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS stg_raw_games (
                id TEXT,
                slug TEXT,
                name TEXT,
                metacritic TEXT,
                released TEXT,
                tba TEXT,
                updated TEXT,
                website TEXT,
                rating TEXT,
                rating_top TEXT,
                playtime TEXT,
                achievements_count TEXT,
                ratings_count TEXT,
                suggestions_count TEXT,
                game_series_count TEXT,
                reviews_count TEXT,
                platforms TEXT,
                developers TEXT,
                genres TEXT,
                publishers TEXT,
                esrb_rating TEXT,
                added_status_yet TEXT,
                added_status_owned TEXT,
                added_status_beaten TEXT,
                added_status_toplay TEXT,
                added_status_dropped TEXT,
                added_status_playing TEXT,
                load_ts TIMESTAMP DEFAULT NOW()
            );
        """
    )

    def download_dataset(**context) -> None:
        try:
            path = dataset_download(
                'jummyegg/rawg-game-dataset', path='game_info.csv'
            )
            context['ti'].xcom_push(key='dataset_path', value=path)
            logging.info('Dataset donwloaded')
        except Exception as e:
            logging.error(f'Kaggle dataset download error {e}')
            raise

    download_game_dataset = PythonOperator(
        task_id='download_game_dataset',
        python_callable=download_dataset
    )

    def load_data_to_raw_stg_table(**context) -> None:
        data_path = context['ti'].xcom_pull(
            key='dataset_path',
            task_ids='download_game_dataset'
        )
        file_path = Path(data_path)
        if not file_path.exists():
            raise FileNotFoundError(f'File not found {file_path}')

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        copy_sql = """
            COPY stg_raw_games (
                id,
                slug,
                name,
                metacritic,
                released,
                tba,
                updated,
                website,
                rating,
                rating_top,
                playtime,
                achievements_count,
                ratings_count,
                suggestions_count,
                game_series_count,
                reviews_count,
                platforms,
                developers,
                genres,
                publishers,
                esrb_rating,
                added_status_yet,
                added_status_owned,
                added_status_beaten,
                added_status_toplay,
                added_status_dropped,
                added_status_playing
            )
            FROM STDIN
            WITH CSV HEADER
                NULL '';
            """

        hook.copy_expert(sql=copy_sql, filename=file_path)

        logging.info('Data loaded into stg_raw_games')

    load_data_to_raw_stg = PythonOperator(
        task_id='load_data_to_raw_stg_game_table',
        python_callable=load_data_to_raw_stg_table
    )

    create_stg_clean = PostgresOperator(
        task_id='create_stg_clean_table',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS stg_games_clean (
                rawg_id                 INT,
                slug                    TEXT,
                name                    TEXT,
                metacritic              INT,
                released_date           TIMESTAMP,
                tba                     BOOLEAN,
                updated_date            TIMESTAMP,
                website                 TEXT,
                rating                  NUMERIC(3,2),
                rating_top              INT,
                playtime_hours          NUMERIC(6,2),
                achievements_count      INT,
                ratings_count           INT,
                suggestions_count       INT,
                game_series_count       INT,
                reviews_count           INT,
                platforms               TEXT,
                developers              TEXT,
                genres                  TEXT,
                publishers              TEXT,
                esrb_rating             TEXT,
                added_status_yet        INT,
                added_status_owned      INT,
                added_status_beaten     INT,
                added_status_toplay     INT,
                added_status_dropped    INT,
                added_status_playing    INT,
                load_ts                 TIMESTAMP
            );
        """
    )

    load_into_clean_stg = PostgresOperator(
        task_id='load_into_clean_stg_games_table',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            INSERT INTO stg_games_clean (
                rawg_id,
                slug,
                name,
                metacritic,
                released_date,
                tba,
                updated_date,
                website,
                rating,
                rating_top,
                playtime_hours,
                achievements_count,
                ratings_count,
                suggestions_count,
                game_series_count,
                reviews_count,
                platforms,
                developers,
                genres,
                publishers,
                esrb_rating,
                added_status_yet,
                added_status_owned,
                added_status_beaten,
                added_status_toplay,
                added_status_dropped,
                added_status_playing,
                load_ts
            )
            SELECT
                id::INT AS rawg_id,
                slug,
                name,
                CASE
                    WHEN metacritic ~ '^[0-9]+$' THEN metacritic::INT
                    ELSE NULL
                END AS metacritic,
                CASE
                    WHEN released IS NULL OR released = '' THEN NULL
                    WHEN lower(released) IN ('n/a', 'na') THEN NULL
                    ELSE released::TIMESTAMP
                END  AS released_date,
                CASE
                   WHEN lower(tba) = 'true' THEN TRUE
                   WHEN lower(tba) = 'false' THEN FALSE
                   ELSE NULL
                END AS tba,
                CASE
                    WHEN updated IS NULL OR updated = '' THEN NULL
                    ELSE updated::TIMESTAMP
                END AS updated_date,
                NULLIF(website, '') AS website,
                CASE
                    WHEN rating ~ '^[0-9]+(\.[0-9]+)?$' THEN rating::NUMERIC(3,2)
                    ELSE NULL
                END AS rating,
                rating_top::INT AS rating_top,
                playtime::NUMERIC(6,2) AS playtime_hours,
                achievements_count::INT AS achievements_count,
                ratings_count::INT AS ratings_count,
                suggestions_count::INT AS suggestions_count,
                game_series_count::INT AS game_series_count,
                reviews_count::INT AS reviews_count,
                NULLIF(platforms, '')  AS platforms,
                NULLIF(developers, '') AS developers,
                NULLIF(genres, '') AS genres,
                NULLIF(publishers, '') AS publishers,
                NULLIF(esrb_rating, '') AS esrb_rating,
                added_status_yet::INT AS added_status_yet,
                added_status_owned::INT AS added_status_owned,
                added_status_beaten::INT AS added_status_beaten,
                added_status_toplay::INT AS added_status_toplay,
                added_status_dropped::INT AS added_status_dropped,
                added_status_playing::INT AS added_status_playing,
                load_ts AS load_ts
            FROM stg_raw_games;
        """
    )

    create_other_stg_tables = PostgresOperator(
        task_id='create_left_other_stg_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS stg_game_platforms (
                rawg_id INT,
                platform_name TEXT
            );

            CREATE TABLE IF NOT EXISTS stg_game_developers (
                rawg_id INT,
                developer_name TEXT
            );

            CREATE TABLE IF NOT EXISTS stg_game_genres (
                rawg_id INT,
                genre_name TEXT
            );

            CREATE TABLE IF NOT EXISTS stg_game_publishers (
               rawg_id INT,
               publisher_name TEXT
            );
        """
    )

    load_into_other_stg_tables = PostgresOperator(
        task_id='load_into_other_left_stg_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            INSERT INTO stg_game_platforms (rawg_id, platform_name)
            SELECT DISTINCT
                rawg_id,
                TRIM(platform_name) AS platform_name
            FROM stg_games_clean
            CROSS JOIN LATERAL unnest(string_to_array(platforms, '||')) AS t(platform_name)
            WHERE platforms IS NOT NULL
            AND platforms <> ''
            AND TRIM(platform_name) <> '';

            INSERT INTO stg_game_developers (rawg_id, developer_name)
            SELECT DISTINCT
                rawg_id,
                TRIM(developer_name) AS developer_name
            FROM stg_games_clean
            CROSS JOIN LATERAL unnest(string_to_array(developers, '||')) AS t(developer_name)
            WHERE developers IS NOT NULL
            AND developers <> ''
            AND TRIM(developer_name) <> '';

            INSERT INTO stg_game_genres (rawg_id, genre_name)
            SELECT DISTINCT
                rawg_id,
                TRIM(genre_name) AS genre_name
            FROM stg_games_clean
            CROSS JOIN LATERAL unnest(string_to_array(genres, '||')) AS t(genre_name)
            WHERE genres IS NOT NULL
            AND genres <> ''
            AND TRIM(genre_name) <> '';

            INSERT INTO stg_game_publishers (rawg_id, publisher_name)
            SELECT DISTINCT
                rawg_id,
                TRIM(publisher_name) AS publisher_name
            FROM stg_games_clean
            CROSS JOIN LATERAL unnest(string_to_array(publishers, '||')) AS t(publisher_name)
            WHERE publishers IS NOT NULL
            AND publishers <> ''
            AND TRIM(publisher_name) <> '';
        """
    )

    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========

    drop_dim_tables = PostgresOperator(
        task_id='drop_dim_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            DROP TABLE IF EXISTS dim_date CASCADE;
            DROP TABLE IF EXISTS dim_esrb_rating CASCADE;
            DROP TABLE IF EXISTS dim_game CASCADE;
            DROP TABLE IF EXISTS dim_platform CASCADE;
            DROP TABLE IF EXISTS dim_developer CASCADE;
            DROP TABLE IF EXISTS dim_genre CASCADE;
            DROP TABLE IF EXISTS dim_publisher CASCADE;
        """
    )

    drop_bridge_tables = PostgresOperator(
        task_id='drop_bridge_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            DROP TABLE IF EXISTS bridge_game_platform CASCADE;
            DROP TABLE IF EXISTS bridge_game_developer CASCADE;
            DROP TABLE IF EXISTS bridge_game_genre CASCADE;
            DROP TABLE IF EXISTS bridge_game_publisher CASCADE;
        """
    )

    create_dim_tables = PostgresOperator(
        task_id='create_dim_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key      INT PRIMARY KEY,
                date_value    DATE NOT NULL,
                year          INT,
                month         INT,
                day           INT,
                month_name    TEXT,
                quarter       INT
            );

            CREATE TABLE IF NOT EXISTS dim_esrb_rating (
                esrb_rating_key  SERIAL PRIMARY KEY,
                esrb_code        TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_game (
                game_key SERIAL PRIMARY KEY,
                rawg_id    INT UNIQUE NOT NULL,
                slug       TEXT,
                name       TEXT,
                website    TEXT,
                tba_flag   BOOLEAN,
                rating_top INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                released_date_key  INT REFERENCES dim_date(date_key),
                updated_date_key   INT REFERENCES dim_date(date_key),
                esrb_rating_key    INT REFERENCES dim_esrb_rating(esrb_rating_key)
            );

            CREATE TABLE IF NOT EXISTS dim_platform (
                platform_key   SERIAL PRIMARY KEY,
                platform_name  TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_developer (
                developer_key  SERIAL PRIMARY KEY,
                developer_name TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_genre (
                genre_key   SERIAL PRIMARY KEY,
                genre_name  TEXT UNIQUE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_publisher (
                publisher_key   SERIAL PRIMARY KEY,
                publisher_name  TEXT UNIQUE NOT NULL
            );
        """
    )

    create_fact_game_table = PostgresOperator(
        task_id='create_fact_game_table',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            DROP TABLE IF EXISTS fact_game_metrics CASCADE;

            CREATE TABLE IF NOT EXISTS fact_game_metrics (
                fact_game_key          SERIAL PRIMARY KEY,
                game_key               INT NOT NULL REFERENCES dim_game(game_key),
                metacritic_score       INT,
                user_rating            NUMERIC(3,2),
                playtime_hours         NUMERIC(6,2),
                achievements_count     INT,
                ratings_count          INT,
                suggestions_count      INT,
                game_series_count      INT,
                reviews_count          INT,
                added_status_yet       INT,
                added_status_owned     INT,
                added_status_beaten    INT,
                added_status_toplay    INT,
                added_status_dropped   INT,
                added_status_playing   INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    create_bridge_tables = PostgresOperator(
        task_id='create_bridge_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            CREATE TABLE IF NOT EXISTS bridge_game_platform (
                fact_game_key  INT REFERENCES fact_game_metrics(fact_game_key),
                platform_key   INT REFERENCES dim_platform(platform_key),
                PRIMARY KEY (fact_game_key, platform_key)
            );

            CREATE TABLE IF NOT EXISTS bridge_game_developer (
                fact_game_key  INT REFERENCES fact_game_metrics(fact_game_key),
                developer_key  INT REFERENCES dim_developer(developer_key),
                PRIMARY KEY (fact_game_key, developer_key)
            );

            CREATE TABLE IF NOT EXISTS bridge_game_genre (
                fact_game_key  INT REFERENCES fact_game_metrics(fact_game_key),
                genre_key      INT REFERENCES dim_genre(genre_key),
                PRIMARY KEY (fact_game_key, genre_key)
            );

            CREATE TABLE IF NOT EXISTS bridge_game_publisher (
                fact_game_key   INT REFERENCES fact_game_metrics(fact_game_key),
                publisher_key   INT REFERENCES dim_publisher(publisher_key),
                PRIMARY KEY (fact_game_key, publisher_key)
            );
        """
    )

    def populate_dim_date(**context) -> None:
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
            INSERT INTO dim_date (
                date_key,
                date_value,
                year,
                month,
                day,
                month_name,
                quarter
            )
           SELECT
                TO_CHAR(d, 'YYYYMMDD')::INT as date_key,
                d::DATE AS date_value,
                EXTRACT(YEAR  FROM d)::INT AS year,
                EXTRACT(MONTH FROM d)::INT AS month,
                EXTRACT(DAY   FROM d)::INT AS day,
                TO_CHAR(d, 'Month') AS month_name,
                EXTRACT(QUARTER FROM d)::INT AS quarter
            FROM generate_series(
                '1962-01-01'::date,
                '2033-01-03'::date,
                '1 day'::interval
            ) AS d;
        """

        hook.run(sql)
        logging.info('Populated dim_date dimension table')

    populate_dim_date_table = PythonOperator(
        task_id='populate_dim_date_table',
        python_callable=populate_dim_date
    )

    def load_to_dim_esrb(**context) -> None:
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
            INSERT INTO dim_esrb_rating (esrb_code)
            SELECT DISTINCT
                TRIM(esrb_rating)
            FROM stg_games_clean
            WHERE esrb_rating IS NOT NULL
            AND esrb_rating <> ''
            ON CONFLICT (esrb_code) DO NOTHING;
        """
        hook.run(sql)
        logging.info('Loaded data into dim_esrb_rating table')

    load_to_dim_esrb_table = PythonOperator(
        task_id='load_to_dim_esrb_table',
        python_callable=load_to_dim_esrb
    )

    def load_to_dim_game(**context) -> None:
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
            INSERT INTO dim_game (
                rawg_id,
                slug,
                name,
                website,
                tba_flag,
                rating_top,
                released_date_key,
                updated_date_key,
                esrb_rating_key
            )
            SELECT
                s.rawg_id,
                s.slug,
                s.name,
                s.website,
                s.tba,
                s.rating_top,
                CASE
                    WHEN s.released_date IS NOT NULL THEN
                    TO_CHAR(s.released_date, 'YYYYMMDD')::INTEGER
                ELSE NULL
                    END AS released_date_key,
                CASE
                    WHEN s.updated_date IS NOT NULL THEN
                    TO_CHAR(s.updated_date, 'YYYYMMDD')::INTEGER
                ELSE NULL
                END AS updated_date_key,
                d_e.esrb_rating_key
            FROM stg_games_clean s
            LEFT JOIN dim_esrb_rating d_e
            ON d_e.esrb_code = s.esrb_rating
            ON CONFLICT (rawg_id) DO UPDATE SET
                slug = EXCLUDED.slug,
                name = EXCLUDED.name,
                website = EXCLUDED.website,
                tba_flag = EXCLUDED.tba_flag,
                rating_top = EXCLUDED.rating_top,
                released_date_key = EXCLUDED.released_date_key,
                updated_date_key = EXCLUDED.updated_date_key,
                esrb_rating_key = EXCLUDED.esrb_rating_key,
                updated_at = CURRENT_TIMESTAMP;
        """

        hook.run(sql)
        logging.info('Loaded into dim_game table')

    load_to_dim_game_table = PythonOperator(
        task_id='load_to_dim_game_table',
        python_callable=load_to_dim_game
    )

    load_to_other_dim_table = PostgresOperator(
        task_id='load_to_dim_platform_dev_genre_pub',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
            INSERT INTO dim_platform (platform_name)
            SELECT DISTINCT
                TRIM(platform_name)
            FROM stg_game_platforms
            WHERE platform_name IS NOT NULL
            AND platform_name <> ''
            ON CONFLICT (platform_name) DO NOTHING;

            INSERT INTO dim_developer (developer_name)
            SELECT DISTINCT
                TRIM(developer_name)
            FROM stg_game_developers
            WHERE developer_name IS NOT NULL
            AND developer_name <> ''
            ON CONFLICT (developer_name) DO NOTHING;

            INSERT INTO dim_genre (genre_name)
            SELECT DISTINCT
                TRIM(genre_name)
            FROM stg_game_genres
            WHERE genre_name IS NOT NULL
            AND genre_name <> ''
            ON CONFLICT (genre_name) DO NOTHING;

            INSERT INTO dim_publisher (publisher_name)
            SELECT DISTINCT
                TRIM(publisher_name)
            FROM stg_game_publishers
            WHERE publisher_name IS NOT NULL
            AND publisher_name <> ''
            ON CONFLICT (publisher_name) DO NOTHING;
        """
    )

    def load_to_fact_table(**context) -> None:
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
            INSERT INTO fact_game_metrics (
                game_key,
                metacritic_score,
                user_rating,
                playtime_hours,
                achievements_count,
                ratings_count,
                suggestions_count,
                game_series_count,
                reviews_count,
                added_status_yet,
                added_status_owned,
                added_status_beaten,
                added_status_toplay,
                added_status_dropped,
                added_status_playing
            )
            SELECT
                d.game_key,
                s.metacritic,
                s.rating,
                s.playtime_hours,
                s.achievements_count,
                s.ratings_count,
                s.suggestions_count,
                s.game_series_count,
                s.reviews_count,
                s.added_status_yet,
                s.added_status_owned,
                s.added_status_beaten,
                s.added_status_toplay,
                s.added_status_dropped,
                s.added_status_playing
            FROM stg_games_clean s
            JOIN dim_game d ON d.rawg_id = s.rawg_id;
        """

        hook.run(sql)
        logging.info('Loaded into fact_game_metrics')

    load_to_fact_game_metrics = PythonOperator(
        task_id='load_to_fact_game_metrics',
        python_callable=load_to_fact_table
    )

    def load_into_bridge_tables(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
            INSERT INTO bridge_game_platform (fact_game_key, platform_key)
            SELECT DISTINCT
                f.fact_game_key,
                p.platform_key
            FROM stg_game_platforms s
            JOIN dim_game d ON d.rawg_id = s.rawg_id
            JOIN fact_game_metrics f ON f.game_key = d.game_key
            JOIN dim_platform p ON p.platform_name = s.platform_name;

            INSERT INTO bridge_game_developer (fact_game_key, developer_key)
            SELECT DISTINCT
                f.fact_game_key,
                d_dev.developer_key
            FROM stg_game_developers s
            JOIN dim_game d ON d.rawg_id = s.rawg_id
            JOIN fact_game_metrics f ON f.game_key = d.game_key
            JOIN dim_developer d_dev ON d_dev.developer_name = s.developer_name;

            INSERT INTO bridge_game_genre (fact_game_key, genre_key)
            SELECT DISTINCT
                f.fact_game_key,
                g.genre_key
            FROM stg_game_genres s
            JOIN dim_game d
                ON d.rawg_id = s.rawg_id
            JOIN fact_game_metrics f
                ON f.game_key = d.game_key
            JOIN dim_genre g
                ON g.genre_name = s.genre_name;

            INSERT INTO bridge_game_publisher (fact_game_key, publisher_key)
            SELECT DISTINCT
                f.fact_game_key,
                p.publisher_key
            FROM stg_game_publishers s
            JOIN dim_game d
                ON d.rawg_id = s.rawg_id
            JOIN fact_game_metrics f
                ON f.game_key = d.game_key
            JOIN dim_publisher p
                ON p.publisher_name = s.publisher_name;
        """

        hook.run(sql)
        logging.info('Loaded into bridge tables')

    load_to_bridge_tables_task = PythonOperator(
        task_id='load_to_bridge_tables_task',
        python_callable=load_into_bridge_tables
    )

    # ========== TASK DEPENDENCIES ==========

    chain(
        # Staging layer
        drop_staging_tables, create_raw_games, download_game_dataset,
        load_data_to_raw_stg, create_stg_clean, load_into_clean_stg,
        create_other_stg_tables, load_into_other_stg_tables,
        # Data warehouse layer
        drop_dim_tables, drop_bridge_tables,
        create_dim_tables, create_fact_game_table,
        create_bridge_tables,
        [populate_dim_date_table, load_to_dim_esrb_table],
        load_to_dim_game_table, load_to_other_dim_table,
        load_to_fact_game_metrics, load_to_bridge_tables_task
    )
