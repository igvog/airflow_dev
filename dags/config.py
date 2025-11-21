from datetime import timedelta

# --- ГЛОБАЛЬНЫЕ НАСТРОЙКИ AIRFLOW (для default_args) ---
AIRFLOW_OWNER = 'bekzat'
AIRFLOW_START_DATE = '2023-01-01'

# Параметры надежности (Try/Catch)
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY = timedelta(minutes=5)
SLA_TIME = timedelta(hours=2)

# --- НАСТРОЙКИ ПОДКЛЮЧЕНИЙ И СХЕМ DWH ---
POSTGRES_CONN_ID = 'postgres_dwh'
RAW_SCHEMA = 'raw'
STAGING_SCHEMA = 'staging'
MARTS_SCHEMA = 'marts'
DATA_DIR = '/data' # Путь внутри контейнера Postgres к папке с CSV

# --- НАСТРОЙКИ WEBHOOK (ЗАМЕНИТЬ НА РЕАЛЬНЫЕ ДАННЫЕ) ---
# Если вы не хотите использовать Telegram, оставьте пустые строки.
BOT_TOKEN = "YOUR_BOT_TOKEN" 
CHAT_ID = "YOUR_CHAT_ID" 
# --------------------------------------------------------

# --- КОНФИГУРАЦИЯ ТАБЛИЦ RAW-СЛОЯ (Метаданные для загрузки) ---
TABLES_CONFIG = {
    'orders_dataset': {
        'file': 'olist_orders_dataset.csv', 
        'columns': ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                    'order_approved_at', 'order_delivered_carrier_date', 
                    'order_delivered_customer_date', 'order_estimated_delivery_date']
    },
    'order_items_dataset': {
        'file': 'olist_order_items_dataset.csv', 
        'columns': ['order_id', 'order_item_id', 'product_id', 'seller_id', 
                    'shipping_limit_date', 'price', 'freight_value']
    },
    'order_payments_dataset': {
        'file': 'olist_order_payments_dataset.csv', 
        'columns': ['order_id', 'payment_sequential', 'payment_type', 
                    'payment_installments', 'payment_value']
    },
    'order_reviews_dataset': {
        'file': 'olist_order_reviews_dataset.csv', 
        'columns': ['review_id', 'order_id', 'review_score', 'review_comment_title', 
                    'review_comment_message', 'review_creation_date', 'review_answer_timestamp']
    },
    'customers_dataset': {
        'file': 'olist_customers_dataset.csv', 
        'columns': ['customer_id', 'customer_unique_id', 'customer_zip_code_prefix', 
                    'customer_city', 'customer_state']
    },
    'sellers_dataset': {
        'file': 'olist_sellers_dataset.csv', 
        'columns': ['seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state']
    },
    'products_dataset': {
        'file': 'olist_products_dataset.csv', 
        'columns': ['product_id', 'product_category_name', 'product_name_lenght', 
                    'product_description_lenght', 'product_photos_qty', 'product_weight_g', 
                    'product_length_cm', 'product_height_cm', 'product_width_cm']
    },
    'geolocation_dataset': {
        'file': 'olist_geolocation_dataset.csv', 
        'columns': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng', 
                    'geolocation_city', 'geolocation_state']
    }
}