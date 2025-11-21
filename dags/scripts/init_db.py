import psycopg2
import os

def create_tables():
    """
    Инициализирует структуру схемы 'Звезда' (Star Schema) в PostgreSQL.
    Создает таблицу Staging, таблицы Измерений (Dimensions) и Фактов (Facts)
    """
    conn_params = {
        "host": "postgres-etl-target",
        "database": "etl_db",
        "user": "etl_user",
        "password": "etl_pass",
        "port": "5432"
    }

    
    commands = [
        # 1. Создание схемы
        "CREATE SCHEMA IF NOT EXISTS star;",
        
        # 2. Таблица STAGING (Сырые данные, копия CSV)
        """
        CREATE TABLE IF NOT EXISTS star.stage_sales (
            invoice_no VARCHAR(50),
            stock_code VARCHAR(50),
            description TEXT,
            quantity INTEGER,
            invoice_date TIMESTAMP,
            unit_price NUMERIC,
            customer_id VARCHAR(50),
            country VARCHAR(100)
        );
        """,
        
        # 3. DIM: Товары
        """
        CREATE TABLE IF NOT EXISTS star.dim_products (
            stock_code VARCHAR(50) PRIMARY KEY,
            description TEXT
        );
        """,
        
        # 4. DIM: Клиенты
        """
        CREATE TABLE IF NOT EXISTS star.dim_customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            country VARCHAR(100)
        );
        """,
        
        # 5. FACT: Продажи
        """
        CREATE TABLE IF NOT EXISTS star.fact_sales (
            sales_id SERIAL PRIMARY KEY,
            invoice_no VARCHAR(50),
            invoice_date TIMESTAMP,
            customer_id VARCHAR(50),
            stock_code VARCHAR(50),
            quantity INTEGER,
            unit_price NUMERIC,
            total_amount NUMERIC,
            -- Уникальный ключ для идемпотентности
            UNIQUE(invoice_no, stock_code, customer_id) 
        );
        """
    ]

    try:
        print("Подключение к базе данных...")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        for command in commands:
            cur.execute(command)
            
        conn.commit()
        cur.close()
        conn.close()
        print("SUCCESS: Star Schema tables created successfully!")
        
    except Exception as e:
        print(f"ERROR: Could not create tables. {e}")

if __name__ == "__main__":
    create_tables()