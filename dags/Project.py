import logging
from datetime import datetime, timedelta
import os
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Настраиваем логгер
logger = logging.getLogger("airflow.task")

POSTGRES_CONN_ID = 'postgres_etl_target_conn'

def notify_failure(context):
    """
    Отправляет уведомление при сбое задачи (Имитация).
    """
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    log_url = task_instance.log_url
    
    # Имитация отправки алерта
    logger.error(f"""
    ###################################################
    ALARM! ALARM! PIPELINE FAILED!
    Task: {task_id}
    Date: {context.get('ds')}
    Log URL: {log_url}
    Sending notification to Data Engineering Team...
    ###################################################
    """)

def load_csv_to_postgres(execution_date, **kwargs):
    """
    Загружает ежедневный CSV в таблицу Staging с маппингом колонок.
    """
    date_str = execution_date
    file_path = f"/opt/airflow/data/sales_{date_str}.csv"
    
    logger.info(f"Starting extraction for date: {date_str}")
    

    try:
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} not found. Skipping load.")
            return

        df = pd.read_csv(file_path)
        logger.info(f"Read {len(df)} rows from CSV.")
        
        #Маппинг колонок CSV в схему БД (CamelCase -> snake_case)
        rename_map = {
            'Invoice': 'invoice_no',
            'StockCode': 'stock_code',
            'Description': 'description',
            'Quantity': 'quantity',
            'InvoiceDate': 'invoice_date',
            'Price': 'unit_price',
            'Customer ID': 'customer_id',
            'Country': 'country'
        }
        df.rename(columns=rename_map, inplace=True)
        df.columns = [c.lower() for c in df.columns]
                
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        
        #Замена данных в staging для текущего батча (if_exists='replace')
        df.to_sql('stage_sales', engine, schema='star', if_exists='replace', index=False)
        logger.info("Successfully loaded data to stage_sales.")
        
    except Exception as e:
        logger.error(f"Critical error in load_csv_to_postgres: {e}")
        raise e

# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notify_failure, 
}

with DAG(
    dag_id='retail_etl_pipeline_v2', # Версия 2 (Улучшенная)
    default_args=default_args,
    description='ETL with Star Schema, Logging and Alerting',
    schedule_interval='@daily', 
    start_date=datetime(2010, 12, 1),
    catchup=False,
    tags=['retail', 'star_schema', 'final_project'],
) as dag:

    # 1. Извлечение данных (Mock API)
    extract_data = BashOperator(
        task_id='extract_data',
        bash_command='python /opt/airflow/dags/scripts/generate_data.py --date {{ ds }}'
    )

    # 2. Загрузка сырых данных в Staging
    load_staging = PythonOperator(
        task_id='load_staging',
        python_callable=load_csv_to_postgres,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    # 3. Наполнение Измерений (Логика Upsert / Игнорирование дублей)
    load_dims = PostgresOperator(
        task_id='load_dims',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            INSERT INTO star.dim_products (stock_code, description)
            SELECT DISTINCT stock_code, description FROM star.stage_sales
            ON CONFLICT (stock_code) DO NOTHING;

            INSERT INTO star.dim_customers (customer_id, country)
            SELECT DISTINCT CAST(customer_id AS VARCHAR), country FROM star.stage_sales
            WHERE customer_id IS NOT NULL
            ON CONFLICT (customer_id) DO NOTHING;
        """
    )

    # 4. Наполнение Фактов (Идемпотентная загрузка)
    load_facts = PostgresOperator(
        task_id='load_facts',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            DELETE FROM star.fact_sales 
            WHERE invoice_date::DATE = '{{ ds }}'::DATE;

            INSERT INTO star.fact_sales (invoice_no, invoice_date, customer_id, stock_code, quantity, unit_price, total_amount)
            SELECT 
                invoice_no,
                CAST(invoice_date AS TIMESTAMP),
                CAST(customer_id AS VARCHAR),
                stock_code,
                quantity,
                unit_price,
                quantity * unit_price
            FROM star.stage_sales
            WHERE customer_id IS NOT NULL
            ON CONFLICT (invoice_no, stock_code, customer_id) DO NOTHING;
        """
    )

    extract_data >> load_staging >> load_dims >> load_facts