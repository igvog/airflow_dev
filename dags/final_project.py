"""
ETL Pipeline: SQLite to PostgreSQL Data Warehouse
KaspiLab: Data Factory - Final Project

Description:
    Extract data from SQLite (olist.sqlite), load to PostgreSQL staging tables,
    and build DWH with star schema (dimensions + fact table).

Architecture:
    1. Extract: SQLite -> CSV
    2. Staging: CSV -> PostgreSQL staging tables
    3. Transform & Load: Staging -> DWH (dimensions + facts)
"""

# ========================== Packages ==============================
# Airflow packages
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Standard library packages
from datetime import datetime, timedelta
import os
import logging
from logging.handlers import RotatingFileHandler

# Third-party packages
import sqlite3
import pandas as pd
from typing import Dict


# ========================== Logging Configuration =================
# Create logs directory
log_dir = os.path.join("dags", "logs")
os.makedirs(log_dir, exist_ok=True)

# Setup logger
log_file = os.path.join(log_dir, "etl_dwh_final.log")
logger = logging.getLogger("etl_dwh_final")
logger.setLevel(logging.DEBUG)

# Rotating file handler (5MB max, 3 backups)
handler = RotatingFileHandler(
    log_file, 
    maxBytes=5 * 1024 * 1024, 
    backupCount=3
)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - [%(funcName)s] - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# ========================== DAG Definition ========================
# Default arguments
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

# DAG configuration
with DAG(
    dag_id="etl_dwh_final_project",
    start_date=datetime(2024, 1, 1),  # Parametrized for backfill
    schedule_interval=None,  # Manual trigger
    catchup=False,
    default_args=default_args,
    tags=["final_project", "data-warehouse", "etl"],
    description="ETL Pipeline: SQLite -> Staging -> DWH with star schema",
) as dag:

    # ========================== Extract Layer =====================
    def extract_from_sqlite() -> dict:
        """
        Extracts data from SQLite database and saves tables to CSV files.
        If SQLite doesn't exist, checks for existing CSV files.
        
        Returns:
            dict: Dictionary with CSV file paths and record counts
        """

        data: Dict[str, any] = {}
        db_path = 'dags/files/olist.sqlite'
        files_dir = 'dags/files'
        
        # Check if database exists
        if not os.path.exists(db_path):
            logger.warning(f"SQLite database not found at: {db_path}")
            
            # Check for CSV files as fallback
            logger.info("Checking for existing CSV files...")
            csv_files = [f for f in os.listdir(files_dir) if f.endswith('.csv')]
            
            if len(csv_files) >= 5:
                logger.info(f"Found {len(csv_files)} CSV files: {csv_files}")
                
                # Define required tables
                required_tables = ["orders", "order_items", "customers", "products", "sellers"]
                found_tables = []
                
                for table_name in required_tables:
                    csv_path = os.path.join(files_dir, f"{table_name}.csv")
                    if os.path.exists(csv_path):
                        try:
                            # Read and validate CSV
                            df = pd.read_csv(csv_path)
                            
                            # Basic validation (similar to SQLite validation)
                            if df.empty:
                                logger.warning(f"CSV file {table_name} is empty!")
                                continue
                                
                            # Store metadata
                            data[table_name] = csv_path
                            data[f'records_{table_name}'] = len(df)
                            found_tables.append(table_name)
                            
                            logger.info(f"{table_name}: {len(df):,} records -> {csv_path}")
                            
                        except Exception as e:
                            logger.error(f"Error reading CSV {table_name}: {e}")
                            continue
                    else:
                        logger.warning(f"CSV file not found: {table_name}.csv")
                
                # Check if we have enough tables
                if len(found_tables) >= 5:
                    logger.info(f"Successfully loaded {len(found_tables)} CSV files")
                    return data
                else:
                    error_msg = f"Insufficient CSV files. Found: {len(found_tables)}, required: 5"
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)
            else:
                error_msg = f"SQLite database not found and insufficient CSV files. Found CSV: {len(csv_files)}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
        
        # Original SQLite extraction logic
        try:
            # Connect to SQLite
            with sqlite3.connect(db_path) as conn:
                logger.info(f"Connected to SQLite: {db_path}")
                
                # Extract tables
                try:
                    orders_data = pd.read_sql_query("SELECT * FROM orders;", conn)
                    order_items_data = pd.read_sql_query("SELECT * FROM order_items;", conn)
                    customers_data = pd.read_sql_query("SELECT * FROM customers;", conn)
                    products_data = pd.read_sql_query("SELECT * FROM products;", conn)
                    sellers_data = pd.read_sql_query("SELECT * FROM sellers;", conn)
                    
                except Exception as e:
                    logger.error(f"Error extracting data from SQLite: {e}")
                    raise
                
                # Save to CSV and validate
                for df, name in [
                    (orders_data, "orders"),
                    (order_items_data, "order_items"),
                    (customers_data, "customers"),
                    (products_data, "products"),
                    (sellers_data, "sellers"),
                ]:
                    if df.empty:
                        error_msg = f"Table {name} is empty!"
                        logger.warning(error_msg)
                        raise ValueError(error_msg)
                    
                    # Save to CSV
                    csv_path = os.path.join(files_dir, f"{name}.csv")
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    
                    # Store metadata
                    data[name] = csv_path
                    data[f'records_{name}'] = len(df)
                    
                    logger.info(f"{name}: {len(df):,} records -> {csv_path}")
        
        except Exception as e:
            logger.error(f"Critical error during extraction: {e}")
            raise
        
        return data
    
    extract_task = PythonOperator(
        task_id="extract_from_sqlite",
        python_callable=extract_from_sqlite,
    )

    # ========================== Staging Layer =====================
    create_staging_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        -- Drop existing staging tables
        DROP TABLE IF EXISTS stage_orders CASCADE;
        DROP TABLE IF EXISTS stage_order_items CASCADE;
        DROP TABLE IF EXISTS stage_customers CASCADE;
        DROP TABLE IF EXISTS stage_products CASCADE;
        DROP TABLE IF EXISTS stage_sellers CASCADE;

        -- Staging: Orders
        CREATE TABLE stage_orders (
            order_id VARCHAR PRIMARY KEY,
            customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Staging: Order Items
        CREATE TABLE stage_order_items (
            order_item_id INT,
            order_id VARCHAR,
            product_id VARCHAR,
            seller_id VARCHAR,
            shipping_limit_date TIMESTAMP,
            price DECIMAL(12,2),
            freight_value DECIMAL(12,2),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_item_id, order_id)
        );

        -- Staging: Customers
        CREATE TABLE stage_customers (
            customer_id VARCHAR PRIMARY KEY,
            customer_unique_id VARCHAR,
            customer_zip_code_prefix INT,
            customer_city VARCHAR,
            customer_state VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Staging: Products
        CREATE TABLE stage_products (
            product_id VARCHAR PRIMARY KEY,
            product_category_name VARCHAR,
            product_name_length INT,
            product_description_length INT,
            product_photos_qty INT,
            product_weight_g DECIMAL(12,3),
            product_length_cm DECIMAL(10,3),
            product_height_cm DECIMAL(10,3),
            product_width_cm DECIMAL(10,3),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Staging: Sellers
        CREATE TABLE stage_sellers (
            seller_id VARCHAR PRIMARY KEY,
            seller_zip_code_prefix INT,
            seller_city VARCHAR,
            seller_state VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    # ========================== Load to Staging ===================
    def load_to_staging(table_name: str, staging_table: str, **context):
        """
        Universal function to load CSV data into staging tables.

        Args:
            table_name: Source table name from XCom
            staging_table: Target staging table in PostgreSQL
            context: Airflow context
        """
        logger.info(f"Loading data to {staging_table}...")

        try:
            # Get data from XCom
            data: Dict[str, any] = context['ti'].xcom_pull(task_ids='extract_from_sqlite')
            csv_path = data.get(table_name)
            records_count = data.get(f'records_{table_name}', 0)

            # Validate
            if not csv_path or not os.path.exists(csv_path):
                error_msg = f"CSV file not found: {csv_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)

            if records_count == 0:
                error_msg = f"No records to load into {staging_table}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Load using COPY
            hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')

            # Получаем колонки из CSV
            df = pd.read_csv(csv_path, nrows=0)
            csv_columns = df.columns.tolist()
            columns_str = ", ".join(csv_columns)

            copy_sql = f"COPY {staging_table} ({columns_str}) FROM STDIN WITH CSV HEADER DELIMITER ','"
            
            try:
                hook.copy_expert(copy_sql, filename=csv_path)
                logger.info(f"{staging_table}: loaded {records_count:,} records")
            except Exception as e:
                logger.error(f"Error loading to {staging_table}: {e}")
                raise

        except Exception as e:
            logger.error(f"Failed to load {staging_table}: {e}")
            raise

    # Helper function for products with column renaming
    def load_products_to_staging(**context):
        """
        Loads products data with column name mapping and type casting.
        Converts float columns to integers where needed for PostgreSQL.
        """
        logger.info("Loading products to staging with column mapping and type casting...")

        try:
            data: Dict[str, any] = context['ti'].xcom_pull(task_ids='extract_from_sqlite')
            csv_path = data.get('products')

            if not csv_path or not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found: {csv_path}")

            # Read CSV
            df = pd.read_csv(csv_path)

            # Rename columns if they exist
            rename_map = {
                "product_name_lenght": "product_name_length",
                "product_description_lenght": "product_description_length"
            }
            existing_map = {k: v for k, v in rename_map.items() if k in df.columns}
            if existing_map:
                df.rename(columns=existing_map, inplace=True)
                logger.info(f"Renamed columns: {existing_map}")

            # Cast columns to correct types for PostgreSQL
            int_columns = ["product_name_length", "product_description_length", "product_photos_qty"]
            for col in int_columns:
                if col in df.columns:
                    df[col] = df[col].fillna(0).astype(int)

            # Save corrected CSV
            df.to_csv(csv_path, index=False, encoding='utf-8')

            # Prepare COPY with only CSV columns
            csv_columns = df.columns.tolist()
            columns_str = ", ".join(csv_columns)

            hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
            copy_sql = f"COPY stage_products ({columns_str}) FROM STDIN WITH CSV HEADER DELIMITER ','"

            hook.copy_expert(copy_sql, filename=csv_path)
            logger.info(f"stage_products: loaded {len(df):,} records")

        except Exception as e:
            logger.error(f"Failed to load stage_products: {e}")
            raise

    # Create staging load tasks
    load_orders_task = PythonOperator(
        task_id="load_orders_to_staging",
        python_callable=load_to_staging,
        op_kwargs={'table_name': 'orders', 'staging_table': 'stage_orders'},
        provide_context=True,
    )

    load_order_items_task = PythonOperator(
        task_id="load_order_items_to_staging",
        python_callable=load_to_staging,
        op_kwargs={'table_name': 'order_items', 'staging_table': 'stage_order_items'},
        provide_context=True,
    )

    load_customers_task = PythonOperator(
        task_id="load_customers_to_staging",
        python_callable=load_to_staging,
        op_kwargs={'table_name': 'customers', 'staging_table': 'stage_customers'},
        provide_context=True,
    )

    load_products_task = PythonOperator(
        task_id="load_products_to_staging",
        python_callable=load_products_to_staging,
        provide_context=True,
    )

    load_sellers_task = PythonOperator(
        task_id="load_sellers_to_staging",
        python_callable=load_to_staging,
        op_kwargs={'table_name': 'sellers', 'staging_table': 'stage_sellers'},
        provide_context=True,
    )

    # ========================== DWH Schema ========================
    create_dwh_schema = PostgresOperator(
        task_id='create_dwh_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        -- Drop existing DWH tables
        DROP TABLE IF EXISTS fact_order_items CASCADE;
        DROP TABLE IF EXISTS dim_products CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_customers CASCADE;
        DROP TABLE IF EXISTS dim_sellers CASCADE;

        -- Dimension: Date
        CREATE TABLE dim_date (
            date_key INT PRIMARY KEY,
            date DATE NOT NULL UNIQUE,
            day INT,
            month INT,
            year INT,
            quarter INT,
            week_of_year INT,
            day_of_week INT,
            is_weekend BOOLEAN
        );

        -- Dimension: Customers
        CREATE TABLE dim_customers (
            customer_sk BIGSERIAL PRIMARY KEY,
            customer_id VARCHAR NOT NULL UNIQUE,
            customer_unique_id VARCHAR,
            customer_zip_code_prefix INT,
            customer_city VARCHAR,
            customer_state VARCHAR
        );

        -- Dimension: Products
        CREATE TABLE dim_products (
            product_sk BIGSERIAL PRIMARY KEY,
            product_id VARCHAR NOT NULL UNIQUE,
            product_category_name VARCHAR,
            product_name_length INT,
            product_description_length INT,
            product_photos_qty INT,
            product_weight_g NUMERIC(12,3),
            product_length_cm NUMERIC(10,3),
            product_height_cm NUMERIC(10,3),
            product_width_cm NUMERIC(10,3)
        );

        -- Dimension: Sellers
        CREATE TABLE dim_sellers (
            seller_sk BIGSERIAL PRIMARY KEY,
            seller_id VARCHAR NOT NULL UNIQUE,
            seller_zip_code_prefix INT,
            seller_city VARCHAR,
            seller_state VARCHAR
        );

        -- Fact: Order Items
        CREATE TABLE fact_order_items (
            fact_id BIGSERIAL PRIMARY KEY,
            order_id VARCHAR NOT NULL,
            order_item_id INT NOT NULL,
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            product_sk BIGINT REFERENCES dim_products(product_sk),
            seller_sk BIGINT REFERENCES dim_sellers(seller_sk),
            customer_sk BIGINT REFERENCES dim_customers(customer_sk),
            order_date_key INT REFERENCES dim_date(date_key),
            shipping_limit_date TIMESTAMP,
            price NUMERIC(12,2),
            freight_value NUMERIC(12,2),
            total_amount NUMERIC(12,2),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (order_id, order_item_id)
        );
        """
    )

    # ========================== Populate Dimensions ===============
    def populate_dim_date():
        """Populates date dimension from staging orders."""
        logger.info("Populating dim_date...")
        
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO dim_date (
            date_key, date, day, month, year, quarter, 
            week_of_year, day_of_week, is_weekend
        )
        SELECT DISTINCT
            TO_CHAR(order_purchase_timestamp, 'YYYYMMDD')::INT AS date_key,
            DATE(order_purchase_timestamp) AS date,
            EXTRACT(DAY FROM order_purchase_timestamp) AS day,
            EXTRACT(MONTH FROM order_purchase_timestamp) AS month,
            EXTRACT(YEAR FROM order_purchase_timestamp) AS year,
            EXTRACT(QUARTER FROM order_purchase_timestamp) AS quarter,
            EXTRACT(WEEK FROM order_purchase_timestamp) AS week_of_year,
            EXTRACT(DOW FROM order_purchase_timestamp) AS day_of_week,
            CASE 
                WHEN EXTRACT(DOW FROM order_purchase_timestamp) IN (0, 6) 
                THEN TRUE 
                ELSE FALSE 
            END AS is_weekend
        FROM stage_orders
        WHERE order_purchase_timestamp IS NOT NULL
        ON CONFLICT (date_key) DO NOTHING;
        """
        
        hook.run(sql)
        logger.info("dim_date populated")

    def populate_dim_customers():
        """Populates customers dimension from staging."""
        logger.info("Populating dim_customers...")
        
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO dim_customers (
            customer_id, customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state
        )
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM stage_customers
        ON CONFLICT (customer_id) DO NOTHING;
        """
        
        hook.run(sql)
        logger.info("dim_customers populated")

    def populate_dim_products():
        """Populates products dimension from staging."""
        logger.info("Populating dim_products...")
        
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO dim_products (
            product_id, product_category_name, product_name_length,
            product_description_length, product_photos_qty, product_weight_g,
            product_length_cm, product_height_cm, product_width_cm
        )
        SELECT DISTINCT
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM stage_products
        ON CONFLICT (product_id) DO NOTHING;
        """
        
        hook.run(sql)
        logger.info("dim_products populated")

    def populate_dim_sellers():
        """Populates sellers dimension from staging."""
        logger.info("Populating dim_sellers...")
        
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO dim_sellers (
            seller_id, seller_zip_code_prefix, seller_city, seller_state
        )
        SELECT DISTINCT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM stage_sellers
        ON CONFLICT (seller_id) DO NOTHING;
        """
        
        hook.run(sql)
        logger.info("dim_sellers populated")

    # Create dimension tasks
    dim_date_task = PythonOperator(
        task_id="populate_dim_date",
        python_callable=populate_dim_date,
    )

    dim_customers_task = PythonOperator(
        task_id="populate_dim_customers",
        python_callable=populate_dim_customers,
    )

    dim_products_task = PythonOperator(
        task_id="populate_dim_products",
        python_callable=populate_dim_products,
    )

    dim_sellers_task = PythonOperator(
        task_id="populate_dim_sellers",
        python_callable=populate_dim_sellers,
    )

    # ========================== Populate Fact =====================
    def populate_fact_order_items():
        """
        Populates fact table by joining staging and dimensions.
        Calculates total_amount as derived metric.
        """
        logger.info("Populating fact_order_items...")
        
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO fact_order_items (
            order_id, order_item_id, order_status, order_purchase_timestamp,
            product_sk, seller_sk, customer_sk, order_date_key,
            shipping_limit_date, price, freight_value, total_amount
        )
        SELECT
            soi.order_id,
            soi.order_item_id,
            so.order_status,
            so.order_purchase_timestamp,
            dp.product_sk,
            ds.seller_sk,
            dc.customer_sk,
            TO_CHAR(so.order_purchase_timestamp, 'YYYYMMDD')::INT AS order_date_key,
            soi.shipping_limit_date,
            soi.price,
            soi.freight_value,
            (soi.price + soi.freight_value) AS total_amount
        FROM stage_order_items soi
        INNER JOIN stage_orders so ON soi.order_id = so.order_id
        INNER JOIN dim_products dp ON soi.product_id = dp.product_id
        INNER JOIN dim_sellers ds ON soi.seller_id = ds.seller_id
        INNER JOIN dim_customers dc ON so.customer_id = dc.customer_id
        ON CONFLICT (order_id, order_item_id) DO NOTHING;
        """
        
        hook.run(sql)
        logger.info("fact_order_items populated")

    fact_task = PythonOperator(
        task_id="populate_fact_order_items",
        python_callable=populate_fact_order_items,
    )

    # ========================== DAG Flow ==========================
    extract_task >> create_staging_tables >> [load_orders_task, load_order_items_task, load_customers_task, load_products_task, load_sellers_task] >> create_dwh_schema
    create_dwh_schema >> [dim_date_task, dim_customers_task, dim_products_task, dim_sellers_task] >> fact_task