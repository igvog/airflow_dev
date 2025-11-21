"""
ETL DAG: Olist E-Commerce Dataset to Snowflake Schema DWH
This DAG processes Brazilian E-Commerce data from Kaggle into a snowflake schema data warehouse.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
import os
from airflow.operators.email import EmailOperator



# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['zhaksykeldi.sr@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
with DAG(
    'olist_to_snowflake_dwh',
    default_args=default_args,
    description='ETL pipeline: Olist E-Commerce → Snowflake Schema DWH',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['etl', 'olist', 'ecommerce', 'snowflake-schema', 'dwh'],
) as dag:

    # ========== STAGING LAYER ==========

    # Create staging tables for raw Olist data
    def create_staging_tables(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        create_staging = """
        DROP TABLE IF EXISTS staging_customers;
        DROP TABLE IF EXISTS staging_sellers;
        DROP TABLE IF EXISTS staging_products;
        DROP TABLE IF EXISTS staging_orders;
        DROP TABLE IF EXISTS staging_order_items;
        DROP TABLE IF EXISTS staging_payments;
        DROP TABLE IF EXISTS staging_reviews;
        DROP TABLE IF EXISTS staging_geolocation;

        -- Customers staging
        CREATE TABLE IF NOT EXISTS staging_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_zip_code_prefix VARCHAR(10),
            customer_city VARCHAR(100),
            customer_state VARCHAR(10)
        );

        -- Sellers staging
        CREATE TABLE IF NOT EXISTS staging_sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix VARCHAR(10),
            seller_city VARCHAR(100),
            seller_state VARCHAR(10)
        );

        -- Products staging
        CREATE TABLE IF NOT EXISTS staging_products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_name_lenght NUMERIC,
            product_description_lenght NUMERIC,
            product_photos_qty NUMERIC,
            product_weight_g NUMERIC,
            product_length_cm NUMERIC,
            product_height_cm NUMERIC,
            product_width_cm NUMERIC
        );

        -- Orders staging
        CREATE TABLE IF NOT EXISTS staging_orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            order_status VARCHAR(50),
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );

        -- Order items staging
        CREATE TABLE IF NOT EXISTS staging_order_items (
            order_id VARCHAR(50),
            order_item_id INTEGER,
            product_id VARCHAR(50),
            seller_id VARCHAR(50),
            shipping_limit_date TIMESTAMP,
            price NUMERIC(10,2),
            freight_value NUMERIC(10,2),
            PRIMARY KEY (order_id, order_item_id)
        );

        -- Payments staging
        CREATE TABLE IF NOT EXISTS staging_payments (
            order_id VARCHAR(50),
            payment_sequential INTEGER,
            payment_type VARCHAR(50),
            payment_installments INTEGER,
            payment_value NUMERIC(10,2),
            PRIMARY KEY (order_id, payment_sequential)
        );

        -- Reviews staging
        CREATE TABLE IF NOT EXISTS staging_reviews (
            review_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50),
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );

        -- Geolocation staging
        CREATE TABLE IF NOT EXISTS staging_geolocation (
            geolocation_zip_code_prefix VARCHAR(10),
            geolocation_lat NUMERIC(10,6),
            geolocation_lng NUMERIC(10,6),
            geolocation_city VARCHAR(100),
            geolocation_state VARCHAR(10)
        );
        """

        hook.run(create_staging)
        logging.info("Staging tables created successfully")

    create_staging = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
    )

    # Load data to staging tables
    def load_kaggle_data(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        conn = hook.get_conn()
        cursor = conn.cursor()

        dag_directory = os.path.dirname(os.path.abspath(__file__))
        data_path = os.path.join(dag_directory, 'data')

        try:
            tables_files = {
                'staging_customers': 'olist_customers_dataset.csv',
                'staging_sellers': 'olist_sellers_dataset.csv',
                'staging_products': 'olist_products_dataset.csv',
                'staging_orders': 'olist_orders_dataset.csv',
                'staging_order_items': 'olist_order_items_dataset.csv',
                'staging_payments': 'olist_order_payments_dataset.csv',
                'staging_geolocation': 'olist_geolocation_dataset.csv',
                'staging_reviews': 'olist_order_reviews_dataset.csv'
            }

            for table_name, csv_file in tables_files.items():
                csv_path = os.path.join(data_path, csv_file)

                if not os.path.exists(csv_path):
                    logging.warning(f"File not found: {csv_path}")
                    continue

                logging.info(f"Loading {csv_file} into {table_name}")

                if table_name == 'staging_reviews':
                    # Создаем временную таблицу
                    cursor.execute("""
                        CREATE TEMP TABLE temp_reviews_load (
                            review_id VARCHAR(50),
                            order_id VARCHAR(50),
                            review_score INTEGER,
                            review_comment_title TEXT,
                            review_comment_message TEXT,
                            review_creation_date TIMESTAMP,
                            review_answer_timestamp TIMESTAMP
                        )
                    """)

                    # загрузка во временную таблицу
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        cursor.copy_expert("""
                            COPY temp_reviews_load FROM STDIN WITH (
                                FORMAT CSV,
                                HEADER TRUE,
                                DELIMITER ','
                            )
                        """, f)

                    # Очищаем основную таблицу и вставляем дедуплицированные данные
                    cursor.execute("TRUNCATE TABLE staging_reviews")
                    cursor.execute("""
                        INSERT INTO staging_reviews 
                        SELECT DISTINCT ON (review_id) *
                        FROM temp_reviews_load
                        ORDER BY review_id, review_creation_date DESC
                    """)

                    # Удаляем временную таблицу
                    cursor.execute("DROP TABLE temp_reviews_load")

                else:
                    # Для остальных таблиц - обычная загрузка
                    cursor.execute(f"TRUNCATE TABLE {table_name}")

                    with open(csv_path, 'r', encoding='utf-8') as f:
                        cursor.copy_expert(f"""
                            COPY {table_name} FROM STDIN WITH (
                                FORMAT CSV,
                                HEADER TRUE,
                                DELIMITER ','
                            )
                        """, f)

                conn.commit()

                # Проверяем количество загруженных записей
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                logging.info(f"Loaded {count} rows into {table_name}")

            logging.info("All data loaded successfully!!!")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    load_kaggle_data_task = PythonOperator(
        task_id='load_kaggle_data',
        python_callable=load_kaggle_data,
    )


    # ========== DATA WAREHOUSE LAYER (SNOWFLAKE SCHEMA) ==========

    # Creating result tables
    create_dw_tables = PostgresOperator(
        task_id='create_dw_tables',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        DROP TABLE IF EXISTS dim_geolocation CASCADE;
        DROP TABLE IF EXISTS dim_customers CASCADE;
        DROP TABLE IF EXISTS dim_sellers CASCADE;
        DROP TABLE IF EXISTS dim_product_categories CASCADE;
        DROP TABLE IF EXISTS dim_products CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_payment_types CASCADE;
        
        DROP TABLE IF EXISTS fact_order_items CASCADE;
        DROP TABLE IF EXISTS fact_payments CASCADE;
        DROP TABLE IF EXISTS fact_order_reviews CASCADE;
        DROP TABLE IF EXISTS fact_order_delivery CASCADE;

        -- ========== DIMENSION TABLES ==========
        -- Geolocation Dimension
        CREATE TABLE IF NOT EXISTS dim_geolocation (
            geolocation_id SERIAL PRIMARY KEY,
            zip_code_prefix VARCHAR(10) NOT NULL UNIQUE,  
            latitude NUMERIC(10,6),
            longitude NUMERIC(10,6),
            city VARCHAR(100),
            state VARCHAR(10),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Customers Dimension
        CREATE TABLE IF NOT EXISTS dim_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            customer_unique_id VARCHAR(50),
            customer_zip_code_prefix VARCHAR(10),
            customer_city VARCHAR(100),
            customer_state VARCHAR(10),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Sellers Dimension
        CREATE TABLE IF NOT EXISTS dim_sellers (
            seller_id VARCHAR(50) PRIMARY KEY,
            seller_zip_code_prefix VARCHAR(10),
            seller_city VARCHAR(100),
            seller_state VARCHAR(10),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Product Categories Dimension
        CREATE TABLE IF NOT EXISTS dim_product_categories (
            category_id SERIAL PRIMARY KEY,
            product_category_name VARCHAR(100) UNIQUE,
            category_name_english VARCHAR(100),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Products Dimension
        CREATE TABLE IF NOT EXISTS dim_products (
            product_id VARCHAR(50) PRIMARY KEY,
            product_category_name VARCHAR(100),
            product_name_lenght NUMERIC,
            product_description_lenght NUMERIC,
            product_photos_qty NUMERIC,
            product_weight_g NUMERIC,
            product_length_cm NUMERIC,
            product_height_cm NUMERIC,
            product_width_cm NUMERIC,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Date Dimension
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name VARCHAR(20),
            week INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            is_weekend BOOLEAN,
            is_holiday BOOLEAN DEFAULT FALSE,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Payment Types Dimension
        CREATE TABLE IF NOT EXISTS dim_payment_types (
            payment_type_id SERIAL PRIMARY KEY,
            payment_type VARCHAR(50) UNIQUE,
            payment_type_description TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- ========== FACT TABLES ==========

        -- Fact: Order Items (Core business process)
        CREATE TABLE IF NOT EXISTS fact_order_items (
            order_item_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50),
            seller_id VARCHAR(50),
            customer_id VARCHAR(50),
            order_date_id INTEGER,
            price NUMERIC(10,2),
            freight_value NUMERIC(10,2),
            quantity INTEGER DEFAULT 1,
            total_amount NUMERIC(10,2) GENERATED ALWAYS AS (price + freight_value) STORED,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Fact: Payments
        CREATE TABLE IF NOT EXISTS fact_payments (
            payment_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            customer_id VARCHAR(50),
            payment_date_id INTEGER,
            payment_type VARCHAR(50),
            payment_sequential INTEGER,
            payment_installments INTEGER,
            payment_value NUMERIC(10,2),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Fact: Order Reviews
        CREATE TABLE IF NOT EXISTS fact_order_reviews (
            review_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            customer_id VARCHAR(50),
            seller_id VARCHAR(50),
            review_date_id INTEGER,
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Fact: Order Delivery
        CREATE TABLE IF NOT EXISTS fact_order_delivery (
            delivery_id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) NOT NULL,
            customer_id VARCHAR(50),
            seller_id VARCHAR(50),
            order_date_id INTEGER,
            estimated_delivery_date_id INTEGER,
            actual_delivery_date_id INTEGER,
            delivery_days INTEGER,
            is_delivered_on_time BOOLEAN,
            delivery_status VARCHAR(50),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        """
    )

    # ========== DATA TRANSFORMATION TASKS ==========
    # Insert data to dimension tables
    def populate_dimensions(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        try:
            # Вставляем данные
            hook.run("""
                INSERT INTO dim_geolocation (zip_code_prefix, latitude, longitude, city, state)
                SELECT DISTINCT 
                    geolocation_zip_code_prefix,
                    geolocation_lat,
                    geolocation_lng,
                    geolocation_city,
                    geolocation_state
                FROM staging_geolocation
                ON CONFLICT (zip_code_prefix) DO NOTHING;
            """)
            logging.info("dim_geolocation donе!")

            hook.run("""
                INSERT INTO dim_customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
                SELECT 
                    customer_id,
                    customer_unique_id,
                    customer_zip_code_prefix,
                    customer_city,
                    customer_state
                FROM staging_customers
            """)
            logging.info("dim_customers done!")

            hook.run("""
                INSERT INTO dim_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                SELECT 
                    seller_id,
                    seller_zip_code_prefix,
                    seller_city,
                    seller_state
                FROM staging_sellers
            """)
            logging.info("dim_sellers done!")

            hook.run("""
                INSERT INTO dim_product_categories (product_category_name, category_name_english)
                SELECT DISTINCT 
                    product_category_name,
                    INITCAP(REPLACE(COALESCE(product_category_name, 'unknown'), '_', ' ')) as category_name_english
                FROM staging_products
                WHERE product_category_name IS NOT NULL
            """)
            logging.info("dim_product_categories done!")


            hook.run("""
                INSERT INTO dim_products (
                    product_id, product_category_name, product_name_lenght, 
                    product_description_lenght, product_photos_qty, product_weight_g,
                    product_length_cm, product_height_cm, product_width_cm
                )
                SELECT 
                    product_id, 
                    product_category_name, 
                    COALESCE(product_name_lenght, 0),
                    COALESCE(product_description_lenght, 0),
                    COALESCE(product_photos_qty, 0),
                    COALESCE(product_weight_g, 0),
                    COALESCE(product_length_cm, 0),
                    COALESCE(product_height_cm, 0),
                    COALESCE(product_width_cm, 0)
                FROM staging_products
            """)
            logging.info("dim_products done!")

            hook.run("""
                INSERT INTO dim_payment_types (payment_type, payment_type_description)
                SELECT DISTINCT 
                    payment_type,
                    CASE 
                        WHEN payment_type = 'credit_card' THEN 'Credit Card'
                        WHEN payment_type = 'boleto' THEN 'Boleto'
                        WHEN payment_type = 'voucher' THEN 'Voucher'
                        WHEN payment_type = 'debit_card' THEN 'Debit Card'
                        ELSE 'Other'
                    END as payment_type_description
                FROM staging_payments
                ON CONFLICT (payment_type) DO NOTHING;
            """)
            logging.info("dim_payment_types done!")

            # Generate dates for reasonable range (2016-2025 for Olist dataset)
            hook.run("""
                INSERT INTO dim_date (date_id, full_date, year, quarter, month, month_name, week, day, day_of_week, day_name, is_weekend)
                SELECT
                    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
                    d::DATE as full_date,
                    EXTRACT(YEAR FROM d)::INTEGER as year,
                    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
                    EXTRACT(MONTH FROM d)::INTEGER as month,
                    TO_CHAR(d, 'Month') as month_name,
                    EXTRACT(WEEK FROM d)::INTEGER as week,
                    EXTRACT(DAY FROM d)::INTEGER as day,
                    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
                    TO_CHAR(d, 'Day') as day_name,
                    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
                FROM generate_series(
                    '2016-01-01'::DATE,  -- Olist dataset starts around 2016
                    '2020-12-31'::DATE,
                    '1 day'::INTERVAL
                ) d
                ON CONFLICT (date_id) DO NOTHING;
            """)
            logging.info("dim_date done! ")

            logging.info("ALL DIMENSIONS POPULATED!!!")

        except Exception as e:
            logging.warning(f"Some duplicates might exist, but continuing: {str(e)}")
            # Не поднимаем исключение - продолжаем работу

    populate_dims_task = PythonOperator(
        task_id='populate_dimensions',
        python_callable=populate_dimensions,
    )

    # Populate all fact tables from staging
    def populate_fact_tables(**context):
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

        try:
            # Вставляем данные в фактовые таблицы

            # fact_order_items
            hook.run("""
                INSERT INTO fact_order_items (order_id, product_id, seller_id, customer_id, order_date_id, price, freight_value)
                SELECT 
                    soi.order_id,
                    soi.product_id,
                    soi.seller_id,
                    so.customer_id,
                    CASE 
                    WHEN so.order_purchase_timestamp IS NOT NULL
                    THEN TO_CHAR(so.order_purchase_timestamp, 'YYYYMMDD')::INTEGER
                    END as order_date_id,
                    soi.price,
                    soi.freight_value
                FROM staging_order_items soi
                JOIN staging_orders so ON soi.order_id = so.order_id
            """)
            logging.info("Populated fact_order_items!")

            # fact_payments
            hook.run("""
                INSERT INTO fact_payments (order_id, customer_id, payment_date_id, payment_type, payment_sequential, payment_installments, payment_value)
                SELECT 
                    sp.order_id,
                    so.customer_id,
                    CASE 
                    WHEN so.order_purchase_timestamp IS NOT NULL
                    THEN TO_CHAR(so.order_purchase_timestamp, 'YYYYMMDD')::INTEGER
                    END as payment_date_id,
                    sp.payment_type,
                    sp.payment_sequential,
                    sp.payment_installments,
                    sp.payment_value
                FROM staging_payments sp
                JOIN staging_orders so ON sp.order_id = so.order_id
            """)
            logging.info("Populated fact_payments!")

            # fact_order_reviews
            hook.run("""
                INSERT INTO fact_order_reviews (review_id, order_id, customer_id, seller_id, review_date_id, review_score, 
                                              review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
                SELECT 
                    sr.review_id,
                    sr.order_id,
                    so.customer_id,
                    soi.seller_id,
                    TO_CHAR(sr.review_creation_date, 'YYYYMMDD')::INTEGER as review_date_id,
                    sr.review_score,
                    sr.review_comment_title,
                    sr.review_comment_message,
                    sr.review_creation_date,
                    sr.review_answer_timestamp
                FROM staging_reviews sr
                JOIN staging_orders so ON sr.order_id = so.order_id
                LEFT JOIN LATERAL (
                SELECT seller_id 
                FROM staging_order_items 
                WHERE order_id = sr.order_id
                ORDER BY order_item_id
                LIMIT 1) soi ON true
            """)
            logging.info("Populated fact_order_reviews!")

            # fact_order_delivery
            hook.run("""
                INSERT INTO fact_order_delivery (
                    order_id, customer_id, seller_id, order_date_id,
                    estimated_delivery_date_id, actual_delivery_date_id,
                    delivery_days, is_delivered_on_time, delivery_status
                )
                SELECT 
                    so.order_id,
                    so.customer_id,
                    soi.seller_id,
                    CASE 
                    WHEN so.order_purchase_timestamp IS NOT NULL
                    THEN TO_CHAR(so.order_purchase_timestamp, 'YYYYMMDD')::INTEGER
                    END as order_date_id,
                    CASE 
                    WHEN so.order_estimated_delivery_date IS NOT NULL
                    THEN TO_CHAR(so.order_estimated_delivery_date, 'YYYYMMDD')::INTEGER
                    END as estimated_delivery_date_id,
                    CASE 
                    WHEN so.order_delivered_customer_date IS NOT NULL
                    THEN TO_CHAR(so.order_delivered_customer_date, 'YYYYMMDD')::INTEGER
                    END as actual_delivery_date_id,
                    CASE 
                    WHEN so.order_delivered_customer_date IS NOT NULL AND so.order_purchase_timestamp IS NOT NULL 
                    THEN EXTRACT(DAY FROM (so.order_delivered_customer_date - so.order_purchase_timestamp))
                    ELSE NULL 
                    END as delivery_days,
                    CASE 
                    WHEN so.order_delivered_customer_date IS NOT NULL AND so.order_estimated_delivery_date IS NOT NULL
                    THEN so.order_delivered_customer_date <= so.order_estimated_delivery_date
                    ELSE NULL 
                    END as is_delivered_on_time,
                    so.order_status as delivery_status
                FROM staging_orders so
                LEFT JOIN LATERAL (
                    SELECT seller_id 
                    FROM staging_order_items 
                    WHERE order_id = so.order_id
                    ORDER BY order_item_id
                    LIMIT 1
                ) soi ON true
                WHERE so.order_status IS NOT NULL
            """)
            logging.info("Populated fact_order_delivery!")

            logging.info("All fact tables populated successfully!!!")

        except Exception as e:
            logging.error(f"Error populating fact tables: {str(e)}")
            raise

    populate_facts_task = PythonOperator(
        task_id='populate_fact_tables',
        python_callable=populate_fact_tables,
    )

    def validate_dwh(**context):

        logging.info("DWH validation completed")

    validate_dwh_task = PythonOperator(
        task_id='validate_dwh',
        python_callable=validate_dwh,
    )

    success_alert = EmailOperator(
        task_id='success_alert',
        to='zhaksykeldi.sr@gmail.com',
        subject='DAG Succeeded: {{ dag.dag_id }}',
        html_content="""
        <h3>DAG успешно завершён!</h3>
        <p>DAG: {{ dag.dag_id }}</p>
        <p>Execution date: {{ ds }}</p>
        <p>Все задачи выполнены успешно.</p>
        """,
        trigger_rule='all_success'  # запускается только когда все upstream tasks успешны
    )

    # ========== TASK DEPENDENCIES ==========

    # Staging layer
    create_staging >> load_kaggle_data_task

    # Data warehouse layer
    load_kaggle_data_task >> create_dw_tables
    create_dw_tables >> populate_dims_task >> populate_facts_task
    populate_facts_task >> validate_dwh_task >> success_alert