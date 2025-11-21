from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

# --- КОНФИГУРАЦИЯ ---
DATA_DIR = '/data'
SCHEMA_NAME = 'raw'
POSTGRES_CONN_ID = 'postgres_dwh'

# Словарь: Имя файла -> Список колонок (все будут TEXT для надежности)
# Мы убрали префикс 'olist_' из имен таблиц для удобства
TABLES_CONFIG = {
    'orders_dataset': {
        'file': 'olist_orders_dataset.csv',
        'columns': [
            'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
            'order_approved_at', 'order_delivered_carrier_date', 
            'order_delivered_customer_date', 'order_estimated_delivery_date'
        ]
    },
    'order_items_dataset': {
        'file': 'olist_order_items_dataset.csv',
        'columns': [
            'order_id', 'order_item_id', 'product_id', 'seller_id', 
            'shipping_limit_date', 'price', 'freight_value'
        ]
    },
    'order_payments_dataset': {
        'file': 'olist_order_payments_dataset.csv',
        'columns': [
            'order_id', 'payment_sequential', 'payment_type', 
            'payment_installments', 'payment_value'
        ]
    },
    'order_reviews_dataset': {
        'file': 'olist_order_reviews_dataset.csv',
        'columns': [
            'review_id', 'order_id', 'review_score', 'review_comment_title', 
            'review_comment_message', 'review_creation_date', 'review_answer_timestamp'
        ]
    },
    'customers_dataset': {
        'file': 'olist_customers_dataset.csv',
        'columns': [
            'customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 
            'customer_city', 'customer_state'
        ]
    },
    'sellers_dataset': {
        'file': 'olist_sellers_dataset.csv',
        'columns': [
            'seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state'
        ]
    },
    'products_dataset': {
        'file': 'olist_products_dataset.csv',
        'columns': [
            'product_id', 'product_category_name', 'product_name_lenght', 
            'product_description_lenght', 'product_photos_qty', 'product_weight_g', 
            'product_length_cm', 'product_height_cm', 'product_width_cm'
        ]
    },
    'geolocation_dataset': {
        'file': 'olist_geolocation_dataset.csv',
        'columns': [
            'geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 
            'geolocation_city', 'geolocation_state'
        ]
    }
}

default_args = {
    'owner': 'bekzat',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='02_ingest_all_raw_data',
    default_args=default_args,
    description='Full ingest of Olist dataset to RAW layer',
    schedule_interval=None,
    catchup=False,
    tags=['ingestion', 'raw', 'marketplace'],
) as dag:

    # 1. Создаем схему
    create_schema = PostgresOperator(
        task_id='create_raw_schema',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};"
    )

    # 2. Цикл по всем таблицам
    for table_name, config in TABLES_CONFIG.items():
        csv_file = config['file']
        columns = config['columns']
        csv_path = f"{DATA_DIR}/{csv_file}"
        
        # Формируем список колонок для SQL (все TEXT)
        columns_sql_definition = ", ".join([f"{col} TEXT" for col in columns])
        columns_list_str = ", ".join(columns)

        # SQL: Удалить -> Создать -> Загрузить
        # Мы используем одну задачу PostgresOperator с несколькими командами для атомарности
        full_sql_command = f"""
            DROP TABLE IF EXISTS {SCHEMA_NAME}.{table_name} CASCADE;
            
            CREATE TABLE {SCHEMA_NAME}.{table_name} (
                {columns_sql_definition}
            );
            
            COPY {SCHEMA_NAME}.{table_name} ({columns_list_str})
            FROM '{csv_path}' 
            DELIMITER ',' 
            CSV HEADER 
            QUOTE '"';
        """

        load_task = PostgresOperator(
            task_id=f'ingest_{table_name}',
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=full_sql_command,
        )

        create_schema >> load_task