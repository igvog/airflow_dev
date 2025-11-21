"""
DAG для загрузки CSV shopping_behavior_updated.csv в Star Schema DW
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import csv


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

CONN_ID = 'postgres_etl_target_conn'
CSV_PATH = '/opt/airflow/dags/data/shopping_behavior_updated.csv'


def create_staging_table():
    """Создание staging таблицы для загрузки CSV"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    sql = """
    DROP TABLE IF EXISTS staging_shopping_behavior CASCADE;
    
    CREATE TABLE staging_shopping_behavior (
        customer_id INT,
        age INT,
        gender TEXT,
        item_purchased TEXT,
        category TEXT,
        purchase_amount_usd INT,
        location TEXT,
        size TEXT,
        color TEXT,
        season TEXT,
        review_rating FLOAT,
        subscription_status TEXT,
        discount_applied TEXT,
        previous_purchases INT,
        payment_method TEXT,
        frequency_of_purchases TEXT
    );
    """
    
    hook.run(sql)
    print("Staging таблица создана успешно")


def load_csv_to_staging():
    """Загрузка CSV файла в staging таблицу"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        print(f"Открываем файл: {CSV_PATH}")
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            csv_reader = csv.DictReader(f)
            
            # Получаем названия колонок
            fieldnames = csv_reader.fieldnames
            print(f"Колонки в CSV: {fieldnames}")
            
            # Создаём маппинг колонок (на случай разных названий)
            # Пытаемся найти колонку Purchase Amount по частичному совпадению
            purchase_amount_col = None
            for col in fieldnames:
                if 'Purchase Amount' in col:
                    purchase_amount_col = col
                    print(f"Найдена колонка для Purchase Amount: '{purchase_amount_col}'")
                    break
            
            if not purchase_amount_col:
                raise ValueError("Не найдена колонка Purchase Amount в CSV")
            
            insert_sql = f"""
            INSERT INTO staging_shopping_behavior (
                customer_id, age, gender, item_purchased, category,
                purchase_amount_usd, location, size, color, season,
                review_rating, subscription_status, discount_applied,
                previous_purchases, payment_method, frequency_of_purchases
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            """
            
            rows_inserted = 0
            for row_num, row in enumerate(csv_reader, start=2):
                try:
                    # Формируем tuple значений в правильном порядке
                    values = (
                        int(row['Customer ID']),
                        int(row['Age']),
                        row['Gender'],
                        row['Item Purchased'],
                        row['Category'],
                        int(row[purchase_amount_col]),  # Используем найденное название
                        row['Location'],
                        row['Size'],
                        row['Color'],
                        row['Season'],
                        float(row['Review Rating']),
                        row['Subscription Status'],
                        row['Discount Applied'],
                        int(row['Previous Purchases']),
                        row['Payment Method'],
                        row['Frequency of Purchases']
                    )
                    
                    cursor.execute(insert_sql, values)
                    rows_inserted += 1
                    
                    if rows_inserted % 1000 == 0:
                        conn.commit()
                        print(f"Загружено {rows_inserted} строк...")
                        
                except Exception as row_error:
                    print(f"Ошибка в строке {row_num}: {row_error}")
                    print(f"Данные строки: {row}")
                    raise
            
            conn.commit()
            print(f"Загрузка завершена. Всего строк: {rows_inserted}")
            
    except FileNotFoundError:
        print(f"ОШИБКА: Файл не найден по пути {CSV_PATH}")
        print("Проверьте, что файл находится в dags/data/shopping_behavior_updated.csv")
        raise
    except Exception as e:
        conn.rollback()
        print(f"ОШИБКА при загрузке: {type(e).__name__}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def create_dw_schema():
    """Создание dimension и fact таблиц Star Schema"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    sql = """
    -- Удаление существующих таблиц
    DROP TABLE IF EXISTS fact_purchases CASCADE;
    DROP TABLE IF EXISTS dim_customers CASCADE;
    DROP TABLE IF EXISTS dim_products CASCADE;
    DROP TABLE IF EXISTS dim_locations CASCADE;
    DROP TABLE IF EXISTS dim_payment_methods CASCADE;
    DROP TABLE IF EXISTS dim_purchase_frequency CASCADE;
    
    -- Dimension: Customers
    CREATE TABLE dim_customers (
        customer_id INT PRIMARY KEY,
        age INT,
        gender TEXT,
        subscription_status TEXT,
        previous_purchases INT
    );
    
    -- Dimension: Products
    CREATE TABLE dim_products (
        product_key SERIAL PRIMARY KEY,
        item_purchased TEXT,
        category TEXT,
        size TEXT,
        color TEXT,
        season TEXT,
        UNIQUE (item_purchased, category, size, color, season)
    );
    
    -- Dimension: Locations
    CREATE TABLE dim_locations (
        location_key SERIAL PRIMARY KEY,
        location TEXT UNIQUE
    );
    
    -- Dimension: Payment Methods
    CREATE TABLE dim_payment_methods (
        payment_method_key SERIAL PRIMARY KEY,
        payment_method TEXT UNIQUE
    );
    
    -- Dimension: Purchase Frequency
    CREATE TABLE dim_purchase_frequency (
        frequency_key SERIAL PRIMARY KEY,
        frequency_of_purchases TEXT UNIQUE
    );
    
    -- Fact: Purchases
    CREATE TABLE fact_purchases (
        purchase_key SERIAL PRIMARY KEY,
        customer_id INT,
        product_key INT,
        location_key INT,
        payment_method_key INT,
        frequency_key INT,
        purchase_amount_usd INT,
        review_rating FLOAT,
        discount_applied BOOLEAN,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
        FOREIGN KEY (location_key) REFERENCES dim_locations(location_key),
        FOREIGN KEY (payment_method_key) REFERENCES dim_payment_methods(payment_method_key),
        FOREIGN KEY (frequency_key) REFERENCES dim_purchase_frequency(frequency_key)
    );
    """
    
    hook.run(sql)
    print("DW schema создана успешно")


def populate_dimensions():
    """Заполнение dimension таблиц из staging"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    sql = """
    -- Заполнение dim_customers
    INSERT INTO dim_customers (customer_id, age, gender, subscription_status, previous_purchases)
    SELECT DISTINCT
        customer_id,
        age,
        gender,
        subscription_status,
        previous_purchases
    FROM staging_shopping_behavior
    ON CONFLICT (customer_id) DO UPDATE SET
        age = EXCLUDED.age,
        gender = EXCLUDED.gender,
        subscription_status = EXCLUDED.subscription_status,
        previous_purchases = EXCLUDED.previous_purchases;
    
    -- Заполнение dim_products
    INSERT INTO dim_products (item_purchased, category, size, color, season)
    SELECT DISTINCT
        item_purchased,
        category,
        size,
        color,
        season
    FROM staging_shopping_behavior
    ON CONFLICT (item_purchased, category, size, color, season) DO NOTHING;
    
    -- Заполнение dim_locations
    INSERT INTO dim_locations (location)
    SELECT DISTINCT location
    FROM staging_shopping_behavior
    WHERE location IS NOT NULL
    ON CONFLICT (location) DO NOTHING;
    
    -- Заполнение dim_payment_methods
    INSERT INTO dim_payment_methods (payment_method)
    SELECT DISTINCT payment_method
    FROM staging_shopping_behavior
    WHERE payment_method IS NOT NULL
    ON CONFLICT (payment_method) DO NOTHING;
    
    -- Заполнение dim_purchase_frequency
    INSERT INTO dim_purchase_frequency (frequency_of_purchases)
    SELECT DISTINCT frequency_of_purchases
    FROM staging_shopping_behavior
    WHERE frequency_of_purchases IS NOT NULL
    ON CONFLICT (frequency_of_purchases) DO NOTHING;
    """
    
    hook.run(sql)
    print("Dimension таблицы заполнены успешно")


def populate_fact():
    """Заполнение fact таблицы с join на dimensions"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    sql = """
    INSERT INTO fact_purchases (
        customer_id,
        product_key,
        location_key,
        payment_method_key,
        frequency_key,
        purchase_amount_usd,
        review_rating,
        discount_applied
    )
    SELECT
        s.customer_id,
        p.product_key,
        l.location_key,
        pm.payment_method_key,
        pf.frequency_key,
        s.purchase_amount_usd,
        s.review_rating,
        CASE WHEN s.discount_applied = 'Yes' THEN TRUE ELSE FALSE END as discount_applied
    FROM staging_shopping_behavior s
    JOIN dim_products p ON (
        s.item_purchased = p.item_purchased
        AND s.category = p.category
        AND s.size = p.size
        AND s.color = p.color
        AND s.season = p.season
    )
    JOIN dim_locations l ON s.location = l.location
    JOIN dim_payment_methods pm ON s.payment_method = pm.payment_method
    JOIN dim_purchase_frequency pf ON s.frequency_of_purchases = pf.frequency_of_purchases;
    """
    
    hook.run(sql)
    
    # Получаем статистику
    count_sql = "SELECT COUNT(*) FROM fact_purchases;"
    result = hook.get_first(count_sql)
    print(f"Fact таблица заполнена успешно. Всего записей: {result[0]}")


# Определение DAG
with DAG(
    'csv_to_dw_star_schema',
    default_args=default_args,
    description='Загрузка CSV shopping behavior в Star Schema DW',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'csv', 'star_schema'],
) as dag:
    
    # Задача 1: Создание staging таблицы
    task_create_staging = PythonOperator(
        task_id='create_staging',
        python_callable=create_staging_table,
    )
    
    # Задача 2: Загрузка CSV в staging
    task_load_csv = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=load_csv_to_staging,
    )
    
    # Задача 3: Создание DW schema
    task_create_dw = PythonOperator(
        task_id='create_dw_schema',
        python_callable=create_dw_schema,
    )
    
    # Задача 4: Заполнение dimensions
    task_populate_dims = PythonOperator(
        task_id='populate_dims',
        python_callable=populate_dimensions,
    )
    
    # Задача 5: Заполнение fact таблицы
    task_populate_fact = PythonOperator(
        task_id='populate_fact',
        python_callable=populate_fact,
    )
    
    # Определение зависимостей
    task_create_staging >> task_load_csv >> task_create_dw >> task_populate_dims >> task_populate_fact