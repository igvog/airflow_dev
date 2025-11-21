"""
ETL DAG: COVID Data → Postgres Star Schema
+ Error Logging into Postgres etl_error_logs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import io
import logging

# ============================
# FAILURE CALLBACK FUNCTION
# ============================

def log_failure(context):
    """Записывает информацию о сбое таска в etl_error_logs table."""
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    try_number = context['task_instance'].try_number
    error_message = context.get('exception')

    hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

    sql = """
        INSERT INTO etl_error_logs (dag_id, task_id, execution_date, try_number, error_message)
        VALUES (%s, %s, %s, %s, %s);
    """

    hook.run(sql, parameters=(dag_id, task_id, execution_date, try_number, str(error_message)))
    logging.error(f"Logged failure for {task_id} into etl_error_logs")


# ============================
# DEFAULT ARGS
# ============================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['kasymsauyt@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_failure_callback': log_failure,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ============================
# DAG DEFINITION
# ============================

with DAG(
    'covid_etl_star_schema',
    default_args=default_args,
    description='COVID CSV → Postgres → Star Schema + Error Logging',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'covid', 'star-schema', 'logging'],
) as dag:

    # ============================
    # ERROR LOG TABLE
    # ============================

    create_error_log_table = PostgresOperator(
        task_id='create_error_log_table',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS etl_error_logs (
            id SERIAL PRIMARY KEY,
            dag_id TEXT,
            task_id TEXT,
            execution_date TIMESTAMP,
            try_number INTEGER,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    # etl_error_logs: лог ошибок ETL, позволяет отслеживать сбои без проверки логов Airflow

    # ============================
    # STAGING TABLE
    # ============================

    create_staging_table = PostgresOperator(
        task_id='create_staging_table',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        DROP TABLE IF EXISTS staging_covid CASCADE;
        CREATE TABLE staging_covid (
            iso_code TEXT,
            continent TEXT,
            location TEXT,
            date DATE,
            total_cases DOUBLE PRECISION,
            new_cases DOUBLE PRECISION,
            total_deaths DOUBLE PRECISION,
            new_deaths DOUBLE PRECISION,
            total_vaccinations DOUBLE PRECISION,
            people_vaccinated DOUBLE PRECISION,
            people_fully_vaccinated DOUBLE PRECISION,
            population DOUBLE PRECISION
        );
        """
    )
    # staging_covid: промежуточная таблица, хранит "сырые" данные для повторной обработки и трансформаций

    # ============================
    # FETCH + LOAD CSV
    # ============================

    def fetch_and_load_covid_data(**context):
        """Скачивает CSV с COVID данными и загружает в staging_covid"""
        url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        try:
            df = pd.read_csv(url)
            logging.info(f"Fetched {len(df)} rows")

            df_staging = df[['iso_code','continent','location','date','total_cases','new_cases',
                             'total_deaths','new_deaths','total_vaccinations','people_vaccinated',
                             'people_fully_vaccinated','population']]

            buffer = io.StringIO()
            df_staging.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.copy_expert("""
                COPY staging_covid (
                    iso_code, continent, location, date, total_cases, new_cases,
                    total_deaths, new_deaths, total_vaccinations, people_vaccinated,
                    people_fully_vaccinated, population
                ) FROM STDIN WITH CSV
            """, buffer)

            conn.commit()
            cursor.close()

            logging.info(f"Loaded {len(df_staging)} rows into staging_covid")

        except Exception as e:
            logging.error(f"Error during fetch/load: {e}")
            raise

    fetch_load_task = PythonOperator(
        task_id='fetch_and_load_covid_data',
        python_callable=fetch_and_load_covid_data,
    )
    # fetch_and_load_covid_data: скачивание CSV и загрузка в staging_covid

    # ============================
    # CREATE DW TABLES
    # ============================

    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        DROP TABLE IF EXISTS fact_covid CASCADE;
        DROP TABLE IF EXISTS dim_country CASCADE;
        DROP TABLE IF EXISTS dim_dates CASCADE;

        CREATE TABLE dim_country (
            country_key SERIAL PRIMARY KEY,
            iso_code TEXT UNIQUE NOT NULL,
            country_name TEXT,
            continent TEXT,
            population DOUBLE PRECISION,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE dim_dates (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            quarter INTEGER,
            day_of_week INTEGER,
            is_weekend BOOLEAN
        );

        CREATE TABLE fact_covid (
            fact_key SERIAL PRIMARY KEY,
            country_key INTEGER REFERENCES dim_country(country_key),
            date_key INTEGER REFERENCES dim_dates(date_key),
            total_cases DOUBLE PRECISION,
            new_cases DOUBLE PRECISION,
            total_deaths DOUBLE PRECISION,
            new_deaths DOUBLE PRECISION,
            total_vaccinations DOUBLE PRECISION,
            people_vaccinated DOUBLE PRECISION,
            people_fully_vaccinated DOUBLE PRECISION
        );
        """
    )
    # dim_country: справочник стран, избегаем повторного хранения названий стран
    # dim_dates: календарная таблица для аналитики по датам
    # fact_covid: фактологическая таблица с показателями COVID, ссылки на dim_country и dim_dates

    # ============================
    # POPULATE DIM COUNTRY
    # ============================

    def populate_dim_country(**context):
        """Заполняет dim_country данными из staging_covid"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
        INSERT INTO dim_country (iso_code, country_name, continent, population)
        SELECT DISTINCT iso_code, location, continent, population
        FROM staging_covid
        ON CONFLICT (iso_code) DO UPDATE SET
            country_name = EXCLUDED.country_name,
            continent = EXCLUDED.continent,
            population = EXCLUDED.population,
            updated_at = CURRENT_TIMESTAMP;
        """
        hook.run(sql)
        logging.info("Populated dim_country")

    populate_dim_country_task = PythonOperator(
        task_id='populate_dim_country',
        python_callable=populate_dim_country,
    )
    # populate_dim_country: заполнение справочника стран

    # ============================
    # POPULATE DIM DATES
    # ============================

    def populate_dim_dates(**context):
        """Заполняет dim_dates уникальными датами из staging_covid"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
        INSERT INTO dim_dates (date_key, full_date, year, month, day, quarter, day_of_week, is_weekend)
        SELECT
            TO_CHAR(date, 'YYYYMMDD')::INTEGER,
            date,
            EXTRACT(YEAR FROM date)::INTEGER,
            EXTRACT(MONTH FROM date)::INTEGER,
            EXTRACT(DAY FROM date)::INTEGER,
            EXTRACT(QUARTER FROM date)::INTEGER,
            EXTRACT(DOW FROM date)::INTEGER,
            CASE WHEN EXTRACT(DOW FROM date) IN (0,6) THEN TRUE ELSE FALSE END
        FROM (SELECT DISTINCT date FROM staging_covid) d
        ON CONFLICT (date_key) DO NOTHING;
        """
        hook.run(sql)
        logging.info("Populated dim_dates")

    populate_dim_dates_task = PythonOperator(
        task_id='populate_dim_dates',
        python_callable=populate_dim_dates,
    )
    # populate_dim_dates: заполнение календарной таблицы

    # ============================
    # POPULATE FACT COVID
    # ============================

    def populate_fact_covid(**context):
        """Заполняет fact_covid фактологическими данными"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        sql = """
        INSERT INTO fact_covid (
            country_key, date_key, total_cases, new_cases,
            total_deaths, new_deaths,
            total_vaccinations, people_vaccinated, people_fully_vaccinated
        )
        SELECT
            dc.country_key,
            dd.date_key,
            sc.total_cases,
            sc.new_cases,
            sc.total_deaths,
            sc.new_deaths,
            sc.total_vaccinations,
            sc.people_vaccinated,
            sc.people_fully_vaccinated
        FROM staging_covid sc
        JOIN dim_country dc ON sc.iso_code = dc.iso_code
        JOIN dim_dates dd ON sc.date = dd.full_date
        ON CONFLICT DO NOTHING;
        """
        hook.run(sql)
        logging.info("Populated fact_covid")

    populate_fact_task = PythonOperator(
        task_id='populate_fact_covid',
        python_callable=populate_fact_covid,
    )
    # populate_fact_covid: заполнение фактологической таблицы

    # ============================
    # TASK DEPENDENCIES
    # ============================

    create_error_log_table >> create_staging_table
    create_staging_table >> fetch_load_task
    fetch_load_task >> create_dw_schema
    create_dw_schema >> [populate_dim_country_task, populate_dim_dates_task]
    [populate_dim_country_task, populate_dim_dates_task] >> populate_fact_task
