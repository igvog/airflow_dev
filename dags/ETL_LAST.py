from datetime import datetime, timedelta
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- CONFIG ---
CSV_FILE = "/opt/airflow/dags/GoodReads_100k_books.csv"
DB_CONN_ID = "postgres_etl_target_conn"
LOG_FILE = "/opt/airflow/logs/goodreads_etl.log"

# --- LOGGING ---
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

default_args = {
    "owner": "armanda",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "goodreads_etl_full_clean",
    default_args=default_args,
    description="ETL for Goodreads books dataset with table cleanup and full population",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

# --- FUNCTIONS ---
def recreate_tables():
    try:
        hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        logging.info("Dropping old tables if exist")
        cursor.execute("DROP TABLE IF EXISTS fact_books;")
        cursor.execute("DROP TABLE IF EXISTS dim_books;")
        cursor.execute("DROP TABLE IF EXISTS dim_ratings;")

        logging.info("Creating dim_books table")
        cursor.execute("""
            CREATE TABLE dim_books (
                book_id BIGINT PRIMARY KEY,
                title TEXT,
                authors TEXT,
                isbn TEXT,
                isbn13 TEXT,
                language_code TEXT,
                num_pages INT,
                publication_date DATE,
                publisher TEXT
            );
        """)

        logging.info("Creating dim_ratings table")
        cursor.execute("""
            CREATE TABLE dim_ratings (
                book_id BIGINT PRIMARY KEY,
                average_rating FLOAT,
                ratings_count INT,
                text_reviews_count INT
            );
        """)

        logging.info("Creating fact_books table")
        cursor.execute("""
            CREATE TABLE fact_books (
                book_id BIGINT PRIMARY KEY,
                title TEXT,
                authors TEXT,
                isbn TEXT,
                isbn13 TEXT,
                language_code TEXT,
                num_pages INT,
                publication_date DATE,
                publisher TEXT,
                average_rating FLOAT,
                ratings_count INT,
                text_reviews_count INT
            );
        """)

        conn.commit()
        cursor.close()
        logging.info("Tables recreated successfully")
    except Exception as e:
        logging.error(f"Error recreating tables: {e}")
        raise

def insert_data():
    try:
        hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        logging.info("Reading CSV file inside insert_data")
        df = pd.read_csv(CSV_FILE)
        df['book_id'] = df.index + 1  # уникальный ID

        logging.info(f"Inserting {len(df)} rows into dim_books and dim_ratings")

        for _, row in df.iterrows():
            # обработка пустых значений
            pub_date = row.get('publication_date')
            if pd.isna(pub_date) or pub_date == '':
                pub_date = None

            num_pages = row.get('pages')
            if pd.isna(num_pages) or num_pages == '':
                num_pages = None

            avg_rating = row.get('rating')
            if pd.isna(avg_rating) or avg_rating == '':
                avg_rating = None

            ratings_count = row.get('totalratings')
            if pd.isna(ratings_count) or ratings_count == '':
                ratings_count = None

            text_reviews_count = row.get('reviews')
            if pd.isna(text_reviews_count) or text_reviews_count == '':
                text_reviews_count = None

            # dim_books
            cursor.execute("""
                INSERT INTO dim_books (book_id, title, authors, isbn, isbn13, language_code, num_pages, publication_date, publisher)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                row['book_id'], row['title'], row['author'], row.get('isbn',''),
                row.get('isbn13',''), row.get('language_code',''), num_pages,
                pub_date, row.get('publisher','')
            ))

            # dim_ratings
            cursor.execute("""
                INSERT INTO dim_ratings (book_id, average_rating, ratings_count, text_reviews_count)
                VALUES (%s,%s,%s,%s);
            """, (
                row['book_id'], avg_rating, ratings_count, text_reviews_count
            ))

        conn.commit()
        cursor.close()
        logging.info("Data inserted successfully")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise

def populate_fact_books():
    try:
        hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        logging.info("Populating fact_books from dim tables")
        cursor.execute("""
            INSERT INTO fact_books (book_id, title, authors, isbn, isbn13, language_code, num_pages, publication_date, publisher,
                                    average_rating, ratings_count, text_reviews_count)
            SELECT b.book_id, b.title, b.authors, b.isbn, b.isbn13, b.language_code, b.num_pages, b.publication_date, b.publisher,
                   r.average_rating, r.ratings_count, r.text_reviews_count
            FROM dim_books b
            LEFT JOIN dim_ratings r ON b.book_id = r.book_id;
        """)
        conn.commit()
        cursor.close()
        logging.info("fact_books populated successfully")
    except Exception as e:
        logging.error(f"Error populating fact_books: {e}")
        raise

# --- TASKS ---
t1 = PythonOperator(
    task_id='recreate_tables',
    python_callable=recreate_tables,
    dag=dag
)

t2 = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=dag
)

t3 = PythonOperator(
    task_id='populate_fact_books',
    python_callable=populate_fact_books,
    dag=dag
)

# --- DEPENDENCIES ---
t1 >> t2 >> t3
