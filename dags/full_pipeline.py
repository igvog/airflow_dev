
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator 
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import os
import requests
import logging
import config 


log = logging.getLogger(__name__)


# Разрешение пути к SQL-файлам
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))

def read_sql_file(subfolder, filename):
    """Читает SQL файл напрямую из указанной подпапки."""
    file_path = os.path.join(DAG_FOLDER, 'sql', subfolder, filename)
    with open(file_path, 'r') as f:
        return f.read()

def on_failure_alert(context):
    """Отправляет детали ошибки в Telegram через HTTP Webhook."""
    
    log_url = context['ti'].log_url
    message = (
        f"🚨 **Airflow FAILED ALERT (Webhook)** 🚨\n"
        f"**Проект:** Olist Kaspi Lab\n"
        f"**DAG ID:** {context['dag_run'].dag_id}\n"
        f"**Задача:** {context['task_instance'].task_id}\n"
        f"**Статус:** ❌ ПРОВАЛ\n"
        f"**Лог:** {log_url}"
    )

    telegram_api_url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/sendMessage"
    
    try:
        response = requests.post(telegram_api_url, data={
            "chat_id": config.CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        })
        response.raise_for_status()
        log.info(f"Telegram alert sent successfully. Status: {response.status_code}")
    except Exception as e:
        log.error(f"Failed to send Telegram alert: {e}")

# --- НАСТРОЙКИ DAG (с учетом Production-Ready фич) ---

default_args = {
    'owner': config.AIRFLOW_OWNER,
    'start_date': config.AIRFLOW_START_DATE,
    'retries': config.DEFAULT_RETRIES,
    'retry_delay': config.DEFAULT_RETRY_DELAY,
    'email_on_failure': False, 
    'on_failure_callback': on_failure_alert, 
    'sla': config.SLA_TIME 
}

with DAG(
    dag_id='01_full_pipeline_olist',
    default_args=default_args,
    description='E2E ELT Pipeline: Raw Ingestion -> Staging -> Gold/Marts (RFM)',
    schedule_interval=None,
    catchup=False, 
    tags=['full', 'e2e', 'prod-ready', 'marketplace'],
) as dag:

    start_pipeline = DummyOperator(task_id='START_PIPELINE')
    finish_pipeline = DummyOperator(task_id='FINISH_PIPELINE')
    
    # 1. Инициализация схем
    init_schemas = PostgresOperator(
        task_id='INIT_SCHEMAS',
        postgres_conn_id=config.POSTGRES_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {config.RAW_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {config.STAGING_SCHEMA};
            CREATE SCHEMA IF NOT EXISTS {config.MARTS_SCHEMA};
        """
    )
    
    # --- TASK GROUP 1: RAW INGESTION ---
    with TaskGroup("RAW_INGESTION_LAYER") as raw_layer:
        
        load_tasks = []
        for table_name, cfg in config.TABLES_CONFIG.items():
            csv_file = cfg['file']
            columns = cfg['columns']
            columns_sql_definition = ", ".join([f"{col} TEXT" for col in columns])
            columns_list_str = ", ".join(columns)
            csv_path = f"{config.DATA_DIR}/{csv_file}"

            full_sql_command = f"""
                DROP TABLE IF EXISTS {config.RAW_SCHEMA}.{table_name} CASCADE;
                
                CREATE TABLE {config.RAW_SCHEMA}.{table_name} (
                    {columns_sql_definition}
                );
                
                COPY {config.RAW_SCHEMA}.{table_name} ({columns_list_str})
                FROM '{csv_path}' 
                DELIMITER ',' 
                CSV HEADER 
                QUOTE '"';
            """
            load_task = PostgresOperator(
                task_id=f'ingest_{table_name}',
                postgres_conn_id=config.POSTGRES_CONN_ID,
                sql=full_sql_command,
            )
            load_tasks.append(load_task)

    # --- TASK GROUP 2: STAGING TRANSFORMS ---
    with TaskGroup("STAGING_TRANSFORMS_LAYER") as staging_layer:
        
        # SQL-файлы для Staging (очистка и типизация)
        STG_MODELS = ['stg_orders.sql', 'stg_items.sql', 'stg_products.sql']
        stg_tasks = []
        
        for model in STG_MODELS:
            sql_content = read_sql_file('staging', model)
            task = PostgresOperator(
                task_id=f'build_{model.replace(".sql", "")}',
                postgres_conn_id=config.POSTGRES_CONN_ID,
                sql=sql_content,
            )
            stg_tasks.append(task)
    
    # --- TASK GROUP 3: GOLD MARTS BUILDING ---
    with TaskGroup("GOLD_MARTS_LAYER") as gold_layer:

        # 1. Основная витрина продаж (GMV, AOV)
        build_sales_mart = PostgresOperator(
            task_id='build_mart_sales_items',
            postgres_conn_id=config.POSTGRES_CONN_ID,
            sql=read_sql_file('marts', 'mart_sales_items.sql') 
        )

        # 2. RFM (Customer Segmentation) - Зависит от sales_items
        build_rfm_mart = PostgresOperator(
            task_id='build_mart_rfm',
            postgres_conn_id=config.POSTGRES_CONN_ID,
            sql=read_sql_file('marts', 'mart_rfm.sql') 
        )

        # 3. Data Quality Check - Проверяет финальную витрину
        data_quality_check = SQLCheckOperator(
            task_id='dq_check_sales_metrics',
            conn_id=config.POSTGRES_CONN_ID,
            sql="""
            SELECT 
                CASE 
                    WHEN COUNT(*) < 80000 THEN 0  -- Проверка на достаточность данных
                    WHEN SUM(CASE WHEN gross_merchandise_value < 0 THEN 1 ELSE 0 END) > 0 THEN 0 -- Проверка на аномалии
                    ELSE 1
                END
            FROM marts.sales_items;
            """,
        )

        # Зависимости внутри Gold слоя
        build_sales_mart >> build_rfm_mart >> data_quality_check
    
    # --- ОБЩАЯ ЦЕПОЧКА ЗАВИСИМОСТЕЙ ---
    
    # START -> INIT -> RAW (Все загрузки параллельно)
    start_pipeline >> init_schemas >> raw_layer
    
    # RAW (после всех загрузок) -> STAGING (Все трансформации параллельно)
    raw_layer >> staging_layer
    
    # STAGING (после всех трансформаций) -> GOLD (Начинается построение Marts)
    staging_layer >> gold_layer
    
    # GOLD -> FINISH
    gold_layer >> finish_pipeline