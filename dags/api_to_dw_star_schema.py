"""
ETL DAG: API to Data Warehouse Star Schema
This DAG extracts data from FastAPI, loads it into Postgres,
and transforms it into a star schema data warehouse model.
"""

from datetime import datetime, timedelta
from airflow import DAG #type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator #type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook #type: ignore
from airflow.operators.python import PythonOperator #type: ignore
import requests #type: ignore
import json
import logging

# Custom Plugin to TypeDict Validate json
from typeddicts import Customer, Order, Seller, OrderItem  #type: ignore

PG_CONN = "pg_conn"

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email': ['zhanzakovdamir2000@gmail.com'], #email to alert to in case of failed tasks
    'email_on_failure': True,
    'email_on_retry': True
}

# DAG definition
with DAG(
    'api_to_dw_star_schema',
    default_args=default_args,
    description='ETL pipeline: API → Postgres → Star Schema DW',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'api', 'datawarehouse', 'star-schema'],
) as dag:

    # ========== STAGING LAYER ==========
    
    def create_staging_tables(**context):
        """Create staging tables for raw API data"""
        hook = PostgresHook(postgres_conn_id='pg_conn1')
        
        # Drop existing staging tables
        drop_staging = """
        DROP TABLE IF EXISTS stg_order_items CASCADE;
        DROP TABLE IF EXISTS stg_orders CASCADE;
        DROP TABLE IF EXISTS stg_customers CASCADE;
        DROP TABLE IF EXISTS stg_sellers CASCADE;
        """
        
        # Create staging tables
        create_staging = """
        -- Staging table for order items
        CREATE TABLE stg_order_items (
            order_id TEXT,
            order_item_id TEXT,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TEXT,
            price TEXT,
            freight_value TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(order_id, order_item_id)
        );
        
        -- Staging table for sellers
        CREATE TABLE stg_sellers (
            seller_id TEXT PRIMARY KEY,
            seller_zip_code_prefix TEXT,
            seller_city TEXT,
            seller_state TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Staging table for customers
        CREATE TABLE stg_customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT,
            customer_zip_code_prefix TEXT,
            customer_city TEXT,
            customer_state TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Staging table for orders
        CREATE TABLE stg_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            order_purchase_timestamp TEXT,
            order_approved_at TEXT,
            order_delivered_carrier_date TEXT,
            order_delivered_customer_date TEXT,
            order_estimated_delivery_date TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        """
        
        hook.run(drop_staging)
        hook.run(create_staging)
        logging.info("Staging tables created successfully")
    
    create_staging = PythonOperator(
        task_id='create_staging_tables',
        python_callable=create_staging_tables,
    )
    
    # ====================
    
    def fetch_api_data(**context):
        """Fetch data from JSONPlaceholder API"""
        base_url = "http://fastapi_app:8000"

        #TODO: implement offset and limit def: 100 100
        limit = 100

        try:
            # Fetch order_items
            order_items = requests.get(f"{base_url}/order_items?limit={limit}")
            order_items.raise_for_status()
            order_items_data:list[OrderItem] = order_items.json()
            logging.info(f"Fetched {len(order_items_data)} posts from API")

            # Fetch orders
            orders_response = requests.get(f"{base_url}/orders?limit={limit}")
            orders_response.raise_for_status()
            orders_data:list[Order] = orders_response.json()
            logging.info(f"Fetched {len(orders_data)} orders from API")

            # Fetch customers
            customers_response = requests.get(f"{base_url}/customers?limit={limit}")
            customers_response.raise_for_status()
            customers_data:list[Customer] = customers_response.json()
            logging.info(f"Fetched {len(customers_data)} customers from API")

            # Fetch sellers
            sellers_response = requests.get(f"{base_url}/sellers?limit={limit}")
            sellers_response.raise_for_status()
            sellers_data:list[Seller] = sellers_response.json()
            logging.info(f"Fetched {len(sellers_data)} sellers from API")

            # Store in XCom for next tasks
            context['ti'].xcom_push(key='order_items_data', value=order_items_data)
            context['ti'].xcom_push(key='orders_data', value=orders_data)
            context['ti'].xcom_push(key='customers_data', value=customers_data)
            context['ti'].xcom_push(key='sellers_data', value=sellers_data)
            
            return {
                'order_items_count': len(order_items_data),
                'orders_count': len(orders_data),
                'customers_count': len(customers_data),
                'sellers_count': len(sellers_data)
            }
        except Exception as e:
            logging.error(f"Error fetching API data: {str(e)}")
            raise
    
    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )
    
    def load_order_items_to_staging(**context):
        """Load order_items data into staging table"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        order_items_data = context['ti'].xcom_pull(key='order_items_data', task_ids='fetch_api_data')
        
        for item in order_items_data:
            insert_query = """
            INSERT INTO stg_order_items (
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value,
                raw_data
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, order_item_id) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                seller_id = EXCLUDED.seller_id,
                shipping_limit_date = EXCLUDED.shipping_limit_date,
                price = EXCLUDED.price,
                freight_value = EXCLUDED.freight_value,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters=(
                    item['order_id'],
                    item['order_item_id'],
                    item['product_id'],
                    item['seller_id'],
                    item['shipping_limit_date'],
                    item['price'],
                    item['freight_value'],
                    json.dumps(item)
                )
            )
    
        logging.info(f"Loaded {len(order_items_data)} items into stg_order_items")
    
    load_order_items = PythonOperator(
        task_id='load_order_items_to_staging',
        python_callable=load_order_items_to_staging,
    )

    
    def load_orders_to_staging(**context):
        """Loads orders into staging table"""
        hook = PostgresHook(postgres_conn_id=PG_CONN)
        orders:list[Order] = context['ti'].xcom_pull(key='orders_data', task_ids="fetch_api_data")
        
        for order in orders:
            insert_query="""
                INSERT INTO stg_orders(
                    order_id, 
                    customer_id, 
                    order_status, 
                    order_purchase_timestamp, 
                    order_approved_at, 
                    order_delivered_carrier_date, 
                    order_delivered_customer_date, 
                    order_estimated_delivery_date, 
                    raw_data
                    )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_status = EXCLUDED.order_status,
                    order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
                    order_approved_at = EXCLUDED.order_approved_at,
                    order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
                    order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
                    order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date,
                    raw_data = EXCLUDED.raw_data,
                    loaded_at = CURRENT_TIMESTAMP;
                """
            hook.run(insert_query,
                     parameters=(
                         order['order_id'],
                         order['customer_id'],
                         order['order_approved_at'],
                         order['order_purchase_timestamp'],
                         order['order_approved_at'],
                         order['order_delivered_carrier_date'],
                         order['order_delivered_customer_date'],
                         order['order_estimated_delivery_date'],
                         json.dumps(order)
                         ))
            
        logging.info(f"Loaded {len(orders)} order into staging_orders")

    load_orders = PythonOperator(
        task_id='load_orders_to_staging',
        python_callable=load_orders_to_staging,
    )
    
    def load_customers_to_staging(**context):
        """Load customers data into staging table"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        customers_data = context['ti'].xcom_pull(key='customers_data', task_ids='fetch_api_data')

        for customer in customers_data:
            insert_query = """
            INSERT INTO stg_customers (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                raw_data
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_unique_id = EXCLUDED.customer_unique_id,
                customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
                customer_city = EXCLUDED.customer_city,
                customer_state = EXCLUDED.customer_state,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters=(
                    customer['customer_id'],
                    customer['customer_unique_id'],
                    customer['customer_zip_code_prefix'],
                    customer['customer_city'],
                    customer['customer_state'],
                    json.dumps(customer)
                )
            )
        logging.info(f"Loaded {len(customers_data)} customers into stg_customers")

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_customers_to_staging,
    )
    
    def load_sellers_to_staging(**context):
        """Load sellers data into staging table"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        sellers_data = context['ti'].xcom_pull(key='sellers_data', task_ids='fetch_api_data')

        for seller in sellers_data:
            insert_query = """
            INSERT INTO stg_sellers (
                seller_id,
                seller_zip_code_prefix,
                seller_city,
                seller_state,
                raw_data
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (seller_id) DO UPDATE SET
                seller_zip_code_prefix = EXCLUDED.seller_zip_code_prefix,
                seller_city = EXCLUDED.seller_city,
                seller_state = EXCLUDED.seller_state,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters=(
                    seller['seller_id'],
                    seller['seller_zip_code_prefix'],
                    seller['seller_city'],
                    seller['seller_state'],
                    json.dumps(seller)
                )
            )
        logging.info(f"Loaded {len(sellers_data)} sellers into stg_sellers")
    
    load_sellers = PythonOperator(
        task_id='load_sellers_to_staging',
        python_callable=load_sellers_to_staging,
    )

    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========
    
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='pg_conn',
        sql="""
        -- Drop existing DW tables
        DROP TABLE IF EXISTS fact_order_items CASCADE;
        DROP TABLE IF EXISTS dim_orders CASCADE;
        DROP TABLE IF EXISTS dim_customers CASCADE;
        DROP TABLE IF EXISTS dim_sellers CASCADE;
        
        -- Dimension: Customers
        CREATE TABLE dim_customers (
            customer_id TEXT PRIMARY KEY,
            unique_id TEXT,
            zip_code_prefix TEXT,
            city TEXT,
            state TEXT
        );
        -- Dimension: Sellers
        CREATE TABLE dim_sellers (
            seller_id TEXT PRIMARY KEY,
            zip_code_prefix TEXT,
            city TEXT,
            state TEXT
        );
        -- Dimernsion: orders
        CREATE TABLE dim_orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT, -- REFERENCES dim_customers(customer_id),
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );

        -- Fact: order items (with metrics)
        CREATE TABLE fact_order_items (
            order_id TEXT, -- REFERENCES dim_orders(order_id),
            order_item_id TEXT,
            product_id TEXT,
            seller_id TEXT, -- REFERENCES dim_sellers(seller_id),
            shipping_limit_date TIMESTAMP,
            price NUMERIC(10,2),
            freight_value NUMERIC(10,2),
            total_value NUMERIC(10,2), -- we calculate this
            PRIMARY KEY (order_id, order_item_id)
        );
        
        """,
    )
    
    def populate_dim_customers(**context):
        """Populate customer dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        sql = """
        INSERT INTO dim_customers (customer_id, unique_id, zip_code_prefix, city, state)
        SELECT DISTINCT
            customer_id,
            customer_unique_id, 
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM stg_customers
        ON CONFLICT (customer_id) 
        DO UPDATE SET
            unique_id = EXCLUDED.unique_id,
            zip_code_prefix = EXCLUDED.zip_code_prefix,
            city = EXCLUDED.city,
            state = EXCLUDED.state;
        """
        
        hook.run(sql)
        logging.info("Populated dim_users dimension table")
    
    populate_dim_customers_task = PythonOperator(
        task_id='populate_dim_customers',
        python_callable=populate_dim_customers,
    )
    
    def populate_dim_sellers(**context):
        """Populate sellers dimension table"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        # Generate dates for the next 5 years
        sql = """
        INSERT INTO dim_sellers (
            seller_id, zip_code_prefix, city, state
        )
        SELECT DISTINCT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM stg_sellers
        ON CONFLICT (seller_id) DO UPDATE SET
            zip_code_prefix = EXCLUDED.zip_code_prefix,
            city = EXCLUDED.city,
            state = EXCLUDED.state;
        """
        
        hook.run(sql)
        logging.info("Populated dim_sellers dimension table")
    
    populate_dim_sellers_task = PythonOperator(
        task_id='populate_dim_sellers',
        python_callable=populate_dim_sellers,
    )

    def populate_dim_orders(**context):
        hook = PostgresHook(postgres_conn_id=PG_CONN)

        sql = """
            INSERT INTO dim_orders (
                order_id, 
                customer_id, 
                order_status,
                order_purchase_timestamp, 
                order_approved_at,
                order_delivered_carrier_date, 
                order_delivered_customer_date,
                order_estimated_delivery_date
            )
            SELECT DISTINCT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp::timestamp,
                order_approved_at::timestamp,
                order_delivered_carrier_date::timestamp, 
                order_delivered_customer_date::timestamp,
                order_estimated_delivery_date::timestamp
            FROM stg_orders
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_status = EXCLUDED.order_status,
                order_purchase_timestamp = EXCLUDED.order_purchase_timestamp,
                order_approved_at = EXCLUDED.order_approved_at,
                order_delivered_carrier_date = EXCLUDED.order_delivered_carrier_date,
                order_delivered_customer_date = EXCLUDED.order_delivered_customer_date,
                order_estimated_delivery_date = EXCLUDED.order_estimated_delivery_date;
            """
        
        hook.run(sql)
        logging.info("Populated dim_orders dimension table")

    populate_dim_orders_task = PythonOperator(
        task_id='populate_dim_orders',
        python_callable=populate_dim_orders,
    )

    def populate_fact_order_items(**context):
        """Populate fact table with posts data"""
        hook = PostgresHook(postgres_conn_id='pg_conn')
        
        sql = """
            INSERT INTO fact_order_items (
                order_id,
                order_item_id, 
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value, 
                total_value
            )
            SELECT DISTINCT
                order_id,
                order_item_id,
                product_id, 
                seller_id,
                shipping_limit_date :: timestamp,
                price::numeric,
                freight_value::numeric,
                (price::numeric + freight_value::numeric) AS total_value
            FROM stg_order_items
            ON CONFLICT (order_id, order_item_id) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                seller_id = EXCLUDED.seller_id,
                shipping_limit_date = EXCLUDED.shipping_limit_date,
                price = EXCLUDED.price,
                freight_value = EXCLUDED.freight_value,
                total_value = EXCLUDED.total_value;
        """
        
        hook.run(sql)
        logging.info("Populated fact_order_items fact table")
    
    populate_fact_order_items_task = PythonOperator(
        task_id='populate_fact_order_items',
        python_callable=populate_fact_order_items,
    )
    
    # ========== TASK DEPENDENCIES ==========
    
    # Staging layer
    create_staging >> fetch_data
    fetch_data >> [load_sellers, load_customers, load_orders, load_order_items] >> create_dw_schema
    
    # Data warehouse layer
    create_dw_schema >> [populate_dim_customers_task, populate_dim_orders_task, populate_dim_sellers_task] >> populate_fact_order_items_task

