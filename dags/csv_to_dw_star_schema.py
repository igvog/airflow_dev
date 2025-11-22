import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

logger = logging.getLogger("airflow.task")

# ============================ TELEGRAM ALERT ============================
def send_telegram_message(message: str, success: bool = True):

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        logger.warning("Telegram токен или chat_id не найдены!")
        return

    emoji = "УСПЕШНО" if success else "ОШИБКА"
    text = f"{emoji} *Airflow Alert*\n\n{message}"

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        response = requests.post(
            url,
            data={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
        if response.status_code != 200:
            logger.error(f"Telegram ошибка: {response.text}")
    except Exception as e:
        logger.error(f"Не удалось отправить в Telegram: {e}")

def telegram_success(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    message = (
        f"DAG: *{dag_id}*\n"
        f"Task: `{task_id}`\n"
        f"Status: *УСПЕШНО ЗАВЕРШЕНО*\n"
        f"Время: {execution_date}\n"
        f"[Логи Airflow]({log_url})"
    )
    send_telegram_message(message, success=True)

def telegram_failure(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task").task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    log_url = context.get("task_instance").log_url

    error = str(exception)[:500] + "..." if len(str(exception)) > 500 else str(exception)

    message = (
        f"DAG: *{dag_id}*\n"
        f"Task: `{task_id}`\n"
        f"Status: *ОШИБКА*\n"
        f"Время: {execution_date}\n"
        f"Ошибка: `{error}`\n"
        f"[Логи Airflow]({log_url})"
    )
    send_telegram_message(message, success=False)

# ============================ DAG ============================
with DAG(
    dag_id='car_sales_star_schema_dwh',
    default_args={
        'owner': 'you',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'on_success_callback': telegram_success,
        'on_failure_callback': telegram_failure,
    },
    description='One-time full load + daily incremental: CSV → Star Schema DWH',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=['etl', 'dwh', 'star-schema', 'telegram-alert'],
) as dag:

    # 1. Создание staging
    create_staging = PostgresOperator(
        task_id='create_staging_table',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        DROP TABLE IF EXISTS staging_car_sales CASCADE;
        CREATE TABLE staging_car_sales (
            "Date" DATE,
            salesperson VARCHAR(100),
            "Customer Name" VARCHAR(150),
            "Customer Age" INTEGER,
            "Customer Gender" VARCHAR(20),
            "Car Make" VARCHAR(50),
            "Car Model" VARCHAR(100),
            "Car Year" INTEGER,
            quantity INTEGER,
            "Sale Price" NUMERIC,
            "Cost" NUMERIC,
            "Profit" NUMERIC,
            "Discount" NUMERIC,
            "Payment Method" VARCHAR(50),
            "Commission Rate" NUMERIC,
            "Commission Earned" NUMERIC,
            "Sales Region" VARCHAR(100),
            "Sale Year" INTEGER,
            "Sale Month" VARCHAR(20),
            "Sale Quarter" VARCHAR(10),
            "Day of Week" VARCHAR(20),
            "Season" VARCHAR(20)
        );
        """,
        autocommit=True,
    )

    # 2. Загрузка CSV
    def load_csv_to_staging(**context):
        file_path = "/opt/airflow/data/car_sales_2018_2024_enhanced.csv"
        temp_file = "/tmp/car_sales_staging_upload.csv"
        if not os.path.exists(file_path):
            error_msg = f"CSV файл не найден: {file_path}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        try:
            logger.info(f"Начинаем загрузку файла: {file_path}")
            df = pd.read_csv(file_path, dtype=str)  # читаем как строки — безопаснее
            df = df.replace({"": None, "NULL": None, "NaN": None})

            logger.info(f"Прочитано {len(df):,} строк из CSV")

            hook.run("TRUNCATE TABLE staging_car_sales;")
            logger.info("staging_car_sales очищена")

            df.to_csv(temp_file, index=False, header=False, na_rep="")
            logger.info(f"Данные сохранены во временный файл: {temp_file}")

            copy_sql = """
                COPY staging_car_sales FROM STDIN 
                WITH (FORMAT CSV, DELIMITER ',', NULL '', ESCAPE '"')
            """
            hook.copy_expert(copy_sql, temp_file)

            logger.info(f"Успешно загружено {len(df):,} строк в staging_car_sales через COPY")

        except Exception as e:
            error_msg = f"Ошибка при загрузке CSV в staging: {e}"
            logger.error(error_msg)
            raise AirflowException(error_msg) from e

        finally:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                    logger.info(f"Временный файл удалён: {temp_file}")
                except Exception as cleanup_error:
                    logger.warning(f"Не удалось удалить временный файл {temp_file}: {cleanup_error}")

    load_data = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=load_csv_to_staging,
    )

    # 3. Создание звёздной схемы
    create_star_schema = PostgresOperator(
        task_id='create_star_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        DROP TABLE IF EXISTS fact_car_sales CASCADE;
        DROP TABLE IF EXISTS dim_customer, dim_car, dim_salesperson, dim_date, dim_region CASCADE;

        CREATE TABLE dim_date (
            date_key INTEGER PRIMARY KEY,
            full_date DATE UNIQUE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name VARCHAR(20),
            day_of_week VARCHAR(20),
            season VARCHAR(20),
            is_weekend BOOLEAN
        );

        CREATE TABLE dim_customer (
            customer_key SERIAL PRIMARY KEY,
            customer_name VARCHAR(150) UNIQUE,
            customer_age INTEGER,
            customer_gender VARCHAR(20),
            age_group VARCHAR(20)
        );

        CREATE TABLE dim_car (
            car_key SERIAL PRIMARY KEY,
            make VARCHAR(50),
            model VARCHAR(100),
            year INTEGER,
            UNIQUE(make, model, year)
        );

        CREATE TABLE dim_salesperson (
            salesperson_key SERIAL PRIMARY KEY,
            salesperson_name VARCHAR(100) UNIQUE
        );

        CREATE TABLE dim_region (
            region_key SERIAL PRIMARY KEY,
            sales_region VARCHAR(100) UNIQUE
        );

        CREATE TABLE fact_car_sales (
            fact_id BIGSERIAL PRIMARY KEY,
            date_key INTEGER REFERENCES dim_date(date_key),
            customer_key INTEGER REFERENCES dim_customer(customer_key),
            car_key INTEGER REFERENCES dim_car(car_key),
            salesperson_key INTEGER REFERENCES dim_salesperson(salesperson_key),
            region_key INTEGER REFERENCES dim_region(region_key),
            quantity INTEGER,
            sale_price NUMERIC,
            cost NUMERIC,
            profit NUMERIC,
            discount NUMERIC,
            commission_earned NUMERIC,
            payment_method VARCHAR(50)
        );
        """,
        autocommit=True,
    )

    # 4. Наполнение данными
    populate_dim_date = PostgresOperator(task_id='populate_dim_date', postgres_conn_id='postgres_etl_target_conn', sql="""
        INSERT INTO dim_date (
            date_key, full_date, year, quarter, month, month_name, day_of_week, season, is_weekend
        )
        SELECT DISTINCT
            TO_CHAR("Date", 'YYYYMMDD')::INTEGER,
            "Date",
            "Sale Year",
            COALESCE(
                NULLIF(REGEXP_REPLACE("Sale Quarter", '[^0-9]', '', 'g'), '')::INTEGER,
                CEIL(EXTRACT(MONTH FROM "Date") / 3.0)::INTEGER
            ),
            EXTRACT(MONTH FROM "Date")::INTEGER,
            TRIM("Sale Month"),
            TRIM("Day of Week"),
            TRIM("Season"),
            "Day of Week" IN ('Saturday', 'Sunday')
        FROM staging_car_sales
        WHERE "Date" IS NOT NULL
        ON CONFLICT (date_key) DO NOTHING;
    """)

    populate_dim_car = PostgresOperator(task_id='populate_dim_car', postgres_conn_id='postgres_etl_target_conn', sql="""
        INSERT INTO dim_car (make, model, year)
        SELECT DISTINCT "Car Make", "Car Model", "Car Year"
        FROM staging_car_sales
        ON CONFLICT DO NOTHING;
    """)

    populate_dim_region = PostgresOperator(task_id='populate_dim_region', postgres_conn_id='postgres_etl_target_conn', sql="""
        INSERT INTO dim_region (sales_region)
        SELECT DISTINCT "Sales Region"
        FROM staging_car_sales
        ON CONFLICT DO NOTHING;
    """)

    populate_dim_salesperson = PostgresOperator(task_id='populate_dim_salesperson', postgres_conn_id='postgres_etl_target_conn', sql="""
        INSERT INTO dim_salesperson (salesperson_name)
        SELECT DISTINCT salesperson
        FROM staging_car_sales
        WHERE salesperson IS NOT NULL
        ON CONFLICT DO NOTHING;
    """)

    populate_dim_customer = PostgresOperator(task_id='populate_dim_customer', postgres_conn_id='postgres_etl_target_conn', sql="""
        INSERT INTO dim_customer (customer_name, customer_age, customer_gender, age_group)
        SELECT DISTINCT
            "Customer Name",
            "Customer Age",
            "Customer Gender",
            CASE
                WHEN "Customer Age" < 25 THEN '18-24'
                WHEN "Customer Age" < 35 THEN '25-34'
                WHEN "Customer Age" < 45 THEN '35-44'
                WHEN "Customer Age" < 55 THEN '45-54'
                ELSE '55+'
            END
        FROM staging_car_sales
        ON CONFLICT (customer_name) DO NOTHING;
    """)

    populate_fact = PostgresOperator(task_id='populate_fact_table', postgres_conn_id='postgres_etl_target_conn', sql="""
        INSERT INTO fact_car_sales (
            date_key, customer_key, car_key, salesperson_key, region_key,
            quantity, sale_price, cost, profit, discount, commission_earned, payment_method
        )
        SELECT
            TO_CHAR(s."Date", 'YYYYMMDD')::INTEGER,
            dc.customer_key,
            dcar.car_key,
            dsp.salesperson_key,
            dr.region_key,
            s.quantity,
            s."Sale Price",
            s."Cost",
            s."Profit",
            s."Discount",
            s."Commission Earned",
            s."Payment Method"
        FROM staging_car_sales s
        JOIN dim_date d ON d.full_date = s."Date"
        JOIN dim_customer dc ON dc.customer_name = s."Customer Name"
        JOIN dim_car dcar ON dcar.make = s."Car Make" AND dcar.model = s."Car Model" AND dcar.year = s."Car Year"
        JOIN dim_salesperson dsp ON dsp.salesperson_name = s.salesperson
        JOIN dim_region dr ON dr.sales_region = s."Sales Region";
    """)

    # Зависимости
    create_staging >> load_data >> create_star_schema
    create_star_schema >> [populate_dim_date, populate_dim_car, populate_dim_region, populate_dim_salesperson, populate_dim_customer] >> populate_fact