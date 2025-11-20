"""
ETL DAG: API to Data Warehouse Star Schema
This DAG extracts data from JSONPlaceholder API, loads it into Postgres,
and transforms it into a star schema data warehouse model.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import requests
import json
import logging

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        # Drop existing staging tables
        drop_staging = """
        DROP TABLE IF EXISTS staging_posts CASCADE;
        DROP TABLE IF EXISTS staging_users CASCADE;
        DROP TABLE IF EXISTS staging_comments CASCADE;
        """
        
        # Create staging tables
        create_staging = """
        CREATE TABLE IF NOT EXISTS staging_posts (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            title TEXT,
            body TEXT,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS staging_users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            username TEXT,
            email TEXT,
            phone TEXT,
            website TEXT,
            address JSONB,
            company JSONB,
            raw_data JSONB,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS staging_comments (
            id INTEGER PRIMARY KEY,
            post_id INTEGER,
            name TEXT,
            email TEXT,
            body TEXT,
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
    
    def fetch_api_data(**context):
        """Fetch data from JSONPlaceholder API"""
        base_url = "https://jsonplaceholder.typicode.com"
        
        try:
            # Fetch posts
            posts_response = requests.get(f"{base_url}/posts")
            posts_response.raise_for_status()
            posts_data = posts_response.json()
            logging.info(f"Fetched {len(posts_data)} posts from API")
            
            # Fetch users
            users_response = requests.get(f"{base_url}/users")
            users_response.raise_for_status()
            users_data = users_response.json()
            logging.info(f"Fetched {len(users_data)} users from API")
            
            # Fetch comments
            comments_response = requests.get(f"{base_url}/comments")
            comments_response.raise_for_status()
            comments_data = comments_response.json()
            logging.info(f"Fetched {len(comments_data)} comments from API")
            
            # Store in XCom for next tasks
            context['ti'].xcom_push(key='posts_data', value=posts_data)
            context['ti'].xcom_push(key='users_data', value=users_data)
            context['ti'].xcom_push(key='comments_data', value=comments_data)
            
            return {
                'posts_count': len(posts_data),
                'users_count': len(users_data),
                'comments_count': len(comments_data)
            }
        except Exception as e:
            logging.error(f"Error fetching API data: {str(e)}")
            raise
    
    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )
    
    def load_posts_to_staging(**context):
        """Load posts data into staging table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        posts_data = context['ti'].xcom_pull(key='posts_data', task_ids='fetch_api_data')
        
        for post in posts_data:
            insert_query = """
            INSERT INTO staging_posts (id, user_id, title, body, raw_data)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters=(
                    post['id'],
                    post['userId'],
                    post['title'],
                    post['body'],
                    json.dumps(post)
                )
            )
        
        logging.info(f"Loaded {len(posts_data)} posts into staging_posts")
    
    load_posts = PythonOperator(
        task_id='load_posts_to_staging',
        python_callable=load_posts_to_staging,
    )
    
    def load_users_to_staging(**context):
        """Load users data into staging table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        users_data = context['ti'].xcom_pull(key='users_data', task_ids='fetch_api_data')
        
        for user in users_data:
            insert_query = """
            INSERT INTO staging_users (id, name, username, email, phone, website, address, company, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                website = EXCLUDED.website,
                address = EXCLUDED.address,
                company = EXCLUDED.company,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters=(
                    user['id'],
                    user['name'],
                    user['username'],
                    user['email'],
                    user.get('phone', ''),
                    user.get('website', ''),
                    json.dumps(user.get('address', {})),
                    json.dumps(user.get('company', {})),
                    json.dumps(user)
                )
            )
        
        logging.info(f"Loaded {len(users_data)} users into staging_users")
    
    load_users = PythonOperator(
        task_id='load_users_to_staging',
        python_callable=load_users_to_staging,
    )
    
    def load_comments_to_staging(**context):
        """Load comments data into staging table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        comments_data = context['ti'].xcom_pull(key='comments_data', task_ids='fetch_api_data')
        
        for comment in comments_data:
            insert_query = """
            INSERT INTO staging_comments (id, post_id, name, email, body, raw_data)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                post_id = EXCLUDED.post_id,
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                body = EXCLUDED.body,
                raw_data = EXCLUDED.raw_data,
                loaded_at = CURRENT_TIMESTAMP;
            """
            hook.run(
                insert_query,
                parameters=(
                    comment['id'],
                    comment['postId'],
                    comment['name'],
                    comment['email'],
                    comment['body'],
                    json.dumps(comment)
                )
            )
        
        logging.info(f"Loaded {len(comments_data)} comments into staging_comments")
    
    load_comments = PythonOperator(
        task_id='load_comments_to_staging',
        python_callable=load_comments_to_staging,
    )
    
    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========
    
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        -- Drop existing DW tables
        DROP TABLE IF EXISTS fact_posts CASCADE;
        DROP TABLE IF EXISTS dim_users CASCADE;
        DROP TABLE IF EXISTS dim_dates CASCADE;
        
        -- Dimension: Users
        CREATE TABLE dim_users (
            user_key SERIAL PRIMARY KEY,
            user_id INTEGER UNIQUE NOT NULL,
            username VARCHAR(100),
            name VARCHAR(200),
            email VARCHAR(200),
            phone VARCHAR(50),
            website VARCHAR(200),
            city VARCHAR(100),
            company_name VARCHAR(200),
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            valid_to TIMESTAMP,
            is_current BOOLEAN DEFAULT TRUE
        );
        
        -- Dimension: Dates (for time-based analysis)
        CREATE TABLE dim_dates (
            date_key INTEGER PRIMARY KEY,
            full_date DATE NOT NULL,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name VARCHAR(20),
            day INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(20),
            is_weekend BOOLEAN
        );
        
        -- Fact: Posts (with metrics)
        CREATE TABLE fact_posts (
            post_key SERIAL PRIMARY KEY,
            post_id INTEGER NOT NULL,
            user_key INTEGER REFERENCES dim_users(user_key),
            date_key INTEGER REFERENCES dim_dates(date_key),
            title TEXT,
            body TEXT,
            body_length INTEGER,
            word_count INTEGER,
            comment_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better query performance
        CREATE INDEX idx_fact_posts_user_key ON fact_posts(user_key);
        CREATE INDEX idx_fact_posts_date_key ON fact_posts(date_key);
        CREATE INDEX idx_fact_posts_post_id ON fact_posts(post_id);
        """,
    )
    
    def populate_dim_users(**context):
        """Populate user dimension table from staging"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO dim_users (user_id, username, name, email, phone, website, city, company_name)
        SELECT DISTINCT
            id as user_id,
            username,
            name,
            email,
            phone,
            website,
            address->>'city' as city,
            company->>'name' as company_name
        FROM staging_users
        ON CONFLICT (user_id) DO UPDATE SET
            username = EXCLUDED.username,
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            phone = EXCLUDED.phone,
            website = EXCLUDED.website,
            city = EXCLUDED.city,
            company_name = EXCLUDED.company_name,
            updated_at = CURRENT_TIMESTAMP;
        """
        
        hook.run(sql)
        logging.info("Populated dim_users dimension table")
    
    populate_dim_users_task = PythonOperator(
        task_id='populate_dim_users',
        python_callable=populate_dim_users,
    )
    
    def populate_dim_dates(**context):
        """Populate date dimension table"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        # Generate dates for the next 5 years
        sql = """
        INSERT INTO dim_dates (date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
            d::DATE as full_date,
            EXTRACT(YEAR FROM d)::INTEGER as year,
            EXTRACT(QUARTER FROM d)::INTEGER as quarter,
            EXTRACT(MONTH FROM d)::INTEGER as month,
            TO_CHAR(d, 'Month') as month_name,
            EXTRACT(DAY FROM d)::INTEGER as day,
            EXTRACT(DOW FROM d)::INTEGER as day_of_week,
            TO_CHAR(d, 'Day') as day_name,
            CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend
        FROM generate_series(
            '2020-01-01'::DATE,
            '2025-12-31'::DATE,
            '1 day'::INTERVAL
        ) d
        ON CONFLICT (date_key) DO NOTHING;
        """
        
        hook.run(sql)
        logging.info("Populated dim_dates dimension table")
    
    populate_dim_dates_task = PythonOperator(
        task_id='populate_dim_dates',
        python_callable=populate_dim_dates,
    )
    
    def populate_fact_posts(**context):
        """Populate fact table with posts data"""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        
        sql = """
        INSERT INTO fact_posts (
            post_id, user_key, date_key, title, body, 
            body_length, word_count, comment_count
        )
        SELECT
            sp.id as post_id,
            du.user_key,
            TO_CHAR(sp.loaded_at, 'YYYYMMDD')::INTEGER as date_key,
            sp.title,
            sp.body,
            LENGTH(sp.body) as body_length,
            array_length(string_to_array(sp.body, ' '), 1) as word_count,
            COALESCE(comment_counts.comment_count, 0) as comment_count
        FROM staging_posts sp
        INNER JOIN dim_users du ON sp.user_id = du.user_id
        LEFT JOIN (
            SELECT post_id, COUNT(*) as comment_count
            FROM staging_comments
            GROUP BY post_id
        ) comment_counts ON sp.id = comment_counts.post_id
        ON CONFLICT (post_id) DO UPDATE SET
            user_key = EXCLUDED.user_key,
            date_key = EXCLUDED.date_key,
            title = EXCLUDED.title,
            body = EXCLUDED.body,
            body_length = EXCLUDED.body_length,
            word_count = EXCLUDED.word_count,
            comment_count = EXCLUDED.comment_count,
            updated_at = CURRENT_TIMESTAMP;
        """
        
        hook.run(sql)
        logging.info("Populated fact_posts fact table")
    
    populate_fact_posts_task = PythonOperator(
        task_id='populate_fact_posts',
        python_callable=populate_fact_posts,
    )
    
    # ========== TASK DEPENDENCIES ==========
    
    # Staging layer
    create_staging >> fetch_data
    fetch_data >> [load_posts, load_users, load_comments]
    
    # Data warehouse layer
    [load_posts, load_users, load_comments] >> create_dw_schema
    create_dw_schema >> [populate_dim_users_task, populate_dim_dates_task]
    [populate_dim_users_task, populate_dim_dates_task] >> populate_fact_posts_task

