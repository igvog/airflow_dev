from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator 
from airflow.utils.dates import days_ago
import os
from datetime import timedelta
import requests 
import logging

log = logging.getLogger(__name__)

# --- КОНФИГУРАЦИЯ ---
POSTGRES_CONN_ID = 'postgres_dwh'
DAG_ID = '04_build_gold_layer'

# --- НАСТРОЙКИ WEBHOOK (ЗАМЕНИТЬ НА РЕАЛЬНЫЕ ДАННЫЕ) ---
BOT_TOKEN = "7725677818:AAEAPFMUjAp2jClVS06ZG2vGor6i9K-bdgU" 
CHAT_ID = "1246458268" 
# --------------------------------------------------------

# Разрешение пути к SQL-файлам
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
SQL_PATH_MARTS = os.path.join(DAG_FOLDER, 'sql', 'marts')

def read_sql_file(filename):
    """Читает SQL файл напрямую, используя абсолютный путь."""
    file_path = os.path.join(SQL_PATH_MARTS, filename)
    with open(file_path, 'r') as f:
        return f.read()

# --- ФУНКЦИЯ ОПОВЕЩЕНИЯ (WEBHOOK) ---
def on_failure_alert(context):
    """Отправляет детали ошибки в Telegram через HTTP Webhook."""
    
    dag_id = context['dag_run'].dag_id
    task_id = context['task_instance'].task_id
    exec_date = context['ds']
    log_url = context['ti'].log_url
    
    message = (
        f"🚨 **Airflow FAILED ALERT (Webhook)** 🚨\n"
        f"**Проект:** Olist Kaspi Lab\n"
        f"**DAG ID:** {dag_id}\n"
        f"**Задача:** {task_id}\n"
        f"**Дата:** {exec_date}\n"
        f"**Статус:** ❌ ПРОВАЛ (После {context['ti'].max_tries} попыток)\n"
        f"**Лог:** [Просмотреть логи]({log_url})"
    )

    telegram_api_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    
    try:
        response = requests.post(telegram_api_url, data={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        })
        response.raise_for_status()
        log.info(f"Telegram alert sent successfully. Status: {response.status_code}")
    except Exception as e:
        log.error(f"Failed to send Telegram alert: {e}")


# --- ИНТЕГРАЦИЯ В default_args ---
default_args = {
    'owner': 'bekzat',
    'start_date': days_ago(1),
    'retries': 3, # Try/Catch: 3 попытки
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False, 
    'on_failure_callback': on_failure_alert, # 🔥 АКТИВИРУЕМ WEBHOOK
    'sla': timedelta(hours=2) # SLA: 2 часа на выполнение
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Staging -> Gold (Marts) transformation with RFM and Production Checks',
    schedule_interval=None,
    catchup=False, 
    tags=['modeling', 'gold', 'metrics', 'dq', 'rfm', 'prod-ready'],
) as dag:

    # 1. Создаем схему marts
    create_schema = PostgresOperator(
        task_id='create_marts_schema',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="CREATE SCHEMA IF NOT EXISTS marts;"
    )

    # 2. Строим основную витрину продаж (наша таблица marts.sales_items)
    build_sales_mart = PostgresOperator(
        task_id='build_mart_sales_items',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=read_sql_file('mart_sales_items.sql') 
    )

    # 3. RFM (Customer Segmentation)
    build_rfm_mart = PostgresOperator(
        task_id='build_mart_rfm',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=read_sql_file('mart_rfm.sql') 
    )

    # 4. Проверка Качества (DQ Check)
    data_quality_check = SQLCheckOperator(
        task_id='dq_check_sales_metrics',
        conn_id=POSTGRES_CONN_ID,
        sql="""
        SELECT 
            CASE 
                WHEN COUNT(*) < 80000 THEN 0
                WHEN SUM(CASE WHEN gross_merchandise_value < 0 THEN 1 ELSE 0 END) > 0 THEN 0
                ELSE 1
            END
        FROM marts.sales_items;
        """,
    )

    # 5. Устанавливаем зависимость: 
    create_schema >> build_sales_mart
    build_sales_mart >> build_rfm_mart
    build_rfm_mart >> data_quality_check