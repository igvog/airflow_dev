"""
## DAG - финальный проект по курсу Data Engineering
 - Работа с датасетом с kaggle - https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv
 - Этот DAG берет данные из датасета, загружает в Postgres и создает Star-схему Data Warehouse
 - Используем execution_date как бизнес-день
"""

from datetime import datetime, timedelta
from pathlib import Path
import csv
import json
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Добавляем логгер
logger = logging.getLogger(__name__)

# Наш data-source, базовый путь к CSV в контейнере
DATA_DIR = "/opt/airflow/dags/data"

# Названия файлов
ORDERS_FILE = "olist_orders_dataset.csv"
ORDER_ITEMS_FILE = "olist_order_items_dataset.csv"
PRODUCTS_FILE = "olist_products_dataset.csv"
CUSTOMERS_FILE = "olist_customers_dataset.csv"
PAYMENTS_FILE = "olist_order_payments_dataset.csv"

# Импортим батчами
BATCH_SIZE = 5000

# Дефолтные аргументы
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ["some_email@example.com"],  # TODO: после выполнения основного задания доработать
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
with DAG(
        'iskander_final_project_dag',
        default_args=default_args,
        description='ETL пайплайн: Датасет → Postgres → Star Schema DW',
        schedule_interval='0 0 1 * *',
        start_date=datetime(2016, 9, 1),
        catchup=True,
        max_active_runs=2,
        max_active_tasks=4,
        tags=['etl', 'dataset', 'csv', 'datawarehouse', 'star-schema']
) as dag:

    # ========== STAGING LAYER ==========
    def create_staging_tables(**context):
        """Создание staging-таблиц под CSV."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")

        drop_sql = """
        DROP TABLE IF EXISTS final_staging_orders CASCADE;
        DROP TABLE IF EXISTS final_staging_order_items CASCADE;
        DROP TABLE IF EXISTS final_staging_products CASCADE;
        DROP TABLE IF EXISTS final_staging_customers CASCADE;
        DROP TABLE IF EXISTS final_staging_order_payments CASCADE;
        """

        create_sql = """
                CREATE TABLE final_staging_orders (
                    order_id                       TEXT,
                    customer_id                    TEXT,
                    order_status                   TEXT,
                    order_purchase_timestamp       TIMESTAMP,
                    order_approved_at              TIMESTAMP,
                    order_delivered_carrier_date   TIMESTAMP,
                    order_delivered_customer_date  TIMESTAMP,
                    order_estimated_delivery_date  TIMESTAMP,
                    raw_data                       JSONB,
                    loaded_at                      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE final_staging_order_items (
                    order_id           TEXT,
                    order_item_id      INTEGER,
                    product_id         TEXT,
                    seller_id          TEXT,
                    shipping_limit_date TIMESTAMP,
                    price              NUMERIC(10,2),
                    freight_value      NUMERIC(10,2),
                    raw_data           JSONB,
                    loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE final_staging_products (
                    product_id                  TEXT,
                    product_category_name       TEXT,
                    product_name_lenght         INTEGER,
                    product_description_lenght  INTEGER,
                    product_photos_qty          INTEGER,
                    product_weight_g            INTEGER,
                    product_length_cm           INTEGER,
                    product_height_cm           INTEGER,
                    product_width_cm            INTEGER,
                    raw_data                    JSONB,
                    loaded_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE final_staging_customers (
                    customer_id              TEXT,
                    customer_unique_id       TEXT,
                    customer_zip_code_prefix INTEGER,
                    customer_city            TEXT,
                    customer_state           TEXT,
                    raw_data                 JSONB,
                    loaded_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE final_staging_order_payments (
                    order_id             TEXT,
                    payment_sequential   INTEGER,
                    payment_type         TEXT,
                    payment_installments INTEGER,
                    payment_value        NUMERIC(10,2),
                    raw_data             JSONB,
                    loaded_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """

        try:
            hook.run(drop_sql)
            hook.run(create_sql)
            logger.info("Промежуточные таблицы успешно созданы")
        except Exception as e:
            logger.exception(f"Ошибка при создании промежуточных таблиц: {e}")
            raise


    create_staging = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables, )


    def check_null_values(value):
        """Преобразует пустые строки в NULL."""
        if value is None:
            return None
        value = value.strip()
        return value if value != "" else None


    def load_orders_to_staging(**context):
        """Загрузка olist_orders_dataset.csv в final_staging_orders."""
        hook = PostgresHook(postgres_conn_id='postgres_etl_target_conn')
        file_path = Path(DATA_DIR) / ORDERS_FILE

        if not file_path.exists():
            logger.error(f"Файл заказов не найден: {file_path}")
            raise FileNotFoundError(file_path)

        target_fields = [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "raw_data",
        ]

        try:
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                batch = []
                total_rows = 0

                for row in reader:
                    batch.append((
                        row["order_id"],
                        row["customer_id"],
                        row["order_status"],
                        check_null_values(row["order_purchase_timestamp"]),
                        check_null_values(row["order_approved_at"]),
                        check_null_values(row["order_delivered_carrier_date"]),
                        check_null_values(row["order_delivered_customer_date"]),
                        check_null_values(row["order_estimated_delivery_date"]),
                        json.dumps(row),
                    ))

                    if len(batch) >= BATCH_SIZE:
                        hook.insert_rows(
                            table="final_staging_orders",
                            rows=batch,
                            target_fields=target_fields,
                        )
                        total_rows += len(batch)
                        batch = []

                if batch:
                    hook.insert_rows(
                        table="final_staging_orders",
                        rows=batch,
                        target_fields=target_fields,
                    )
                    total_rows += len(batch)

            logger.info(f"Загружено {total_rows} строк в final_staging_orders")

        except Exception as e:
            logger.exception(f"Ошибка при загрузке CSV заказов: {e}")
            raise


    load_orders = PythonOperator(
        task_id="load_orders_to_staging",
        python_callable=load_orders_to_staging,
    )


    def load_order_items_to_staging(**context):
        """Загрузка olist_order_items_dataset.csv в final_staging_order_items."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        file_path = Path(DATA_DIR) / ORDER_ITEMS_FILE

        if not file_path.exists():
            logger.error(f"Файл позиций заказов не найден: {file_path}")
            raise FileNotFoundError(file_path)

        target_fields = [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "raw_data",
        ]
        try:
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                batch = []
                total_rows = 0

                for row in reader:
                    batch.append((
                        row["order_id"],
                        int(row["order_item_id"]),
                        row["product_id"],
                        row["seller_id"],
                        check_null_values(row["shipping_limit_date"]),
                        check_null_values(row["price"]),
                        check_null_values(row["freight_value"]),
                        json.dumps(row),
                    ))

                    if len(batch) >= BATCH_SIZE:
                        hook.insert_rows(
                            table="final_staging_order_items",
                            rows=batch,
                            target_fields=target_fields,
                        )
                        total_rows += len(batch)
                        batch = []

                if batch:
                    hook.insert_rows(
                        table="final_staging_order_items",
                        rows=batch,
                        target_fields=target_fields,
                    )
                    total_rows += len(batch)

            logger.info(f"Загружено {total_rows} строк в final_staging_order_items")

        except Exception as e:
            logger.exception(f"Ошибка при загрузке CSV позиций заказов: {e}")
            raise


    load_order_items = PythonOperator(
        task_id="load_order_items_to_staging",
        python_callable=load_order_items_to_staging,
    )


    def load_products_to_staging(**context):
        """Загрузка olist_products_dataset.csv в final_staging_products."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        file_path = Path(DATA_DIR) / PRODUCTS_FILE

        if not file_path.exists():
            logger.error(f"Файл товаров не найден: {file_path}")
            raise FileNotFoundError(file_path)

        target_fields = [
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "raw_data",
        ]

        try:
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                batch = []
                total_rows = 0

                for row in reader:
                    batch.append((
                        row["product_id"],
                        check_null_values(row["product_category_name"]),
                        check_null_values(row["product_name_lenght"]),
                        check_null_values(row["product_description_lenght"]),
                        check_null_values(row["product_photos_qty"]),
                        check_null_values(row["product_weight_g"]),
                        check_null_values(row["product_length_cm"]),
                        check_null_values(row["product_height_cm"]),
                        check_null_values(row["product_width_cm"]),
                        json.dumps(row),
                    ))

                    if len(batch) >= BATCH_SIZE:
                        hook.insert_rows(
                            table="final_staging_products",
                            rows=batch,
                            target_fields=target_fields,
                        )
                        total_rows += len(batch)
                        batch = []

                if batch:
                    hook.insert_rows(
                        table="final_staging_products",
                        rows=batch,
                        target_fields=target_fields,
                    )
                    total_rows += len(batch)

            logger.info(f"Загружено {total_rows} строк в final_staging_products")

        except Exception as e:
            logger.exception(f"Ошибка при загрузке CSV товаров: {e}")
            raise


    load_products = PythonOperator(
        task_id="load_products_to_staging",
        python_callable=load_products_to_staging,
    )


    def load_customers_to_staging(**context):
        """Загрузка olist_customers_dataset.csv в final_staging_customers."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        file_path = Path(DATA_DIR) / CUSTOMERS_FILE

        if not file_path.exists():
            logger.error(f"Файл клиентов не найден: {file_path}")
            raise FileNotFoundError(file_path)

        target_fields = [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "raw_data",
        ]

        try:
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                batch = []
                total_rows = 0

                for row in reader:
                    batch.append((
                        row["customer_id"],
                        row["customer_unique_id"],
                        check_null_values(row["customer_zip_code_prefix"]),
                        row["customer_city"],
                        row["customer_state"],
                        json.dumps(row),
                    ))

                    if len(batch) >= BATCH_SIZE:
                        hook.insert_rows(
                            table="final_staging_customers",
                            rows=batch,
                            target_fields=target_fields,
                        )
                        total_rows += len(batch)
                        batch = []

                if batch:
                    hook.insert_rows(
                        table="final_staging_customers",
                        rows=batch,
                        target_fields=target_fields,
                    )
                    total_rows += len(batch)

            logger.info(f"Загружено {total_rows} строк в final_staging_customers")

        except Exception as e:
            logger.exception(f"Ошибка при загрузке CSV клиентов: {e}")
            raise


    load_customers = PythonOperator(
        task_id="load_customers_to_staging",
        python_callable=load_customers_to_staging,
    )


    def load_payments_to_staging(**context):
        """Загрузка olist_order_payments_dataset.csv в final_staging_order_payments."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        file_path = Path(DATA_DIR) / PAYMENTS_FILE

        if not file_path.exists():
            logger.error(f"Файл оплат не найден: {file_path}")
            raise FileNotFoundError(file_path)

        target_fields = [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
            "raw_data",
        ]

        try:
            with file_path.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                batch = []
                total_rows = 0

                for row in reader:
                    batch.append((
                        row["order_id"],
                        int(row["payment_sequential"]),
                        row["payment_type"],
                        check_null_values(row["payment_installments"]),
                        check_null_values(row["payment_value"]),
                        json.dumps(row),
                    ))

                    if len(batch) >= BATCH_SIZE:
                        hook.insert_rows(
                            table="final_staging_order_payments",
                            rows=batch,
                            target_fields=target_fields,
                        )
                        total_rows += len(batch)
                        batch = []

                if batch:
                    hook.insert_rows(
                        table="final_staging_order_payments",
                        rows=batch,
                        target_fields=target_fields,
                    )
                    total_rows += len(batch)

            logger.info(f"Загружено {total_rows} строк в final_staging_order_payments")

        except Exception as e:
            logger.exception(f"Ошибка при загрузке CSV оплат: {e}")
            raise


    load_payments = PythonOperator(
        task_id="load_payments_to_staging",
        python_callable=load_payments_to_staging,
    )

    # ========== DATA WAREHOUSE LAYER (STAR SCHEMA) ==========

    # Схема DWH
    create_dw_schema = PostgresOperator(
        task_id='create_dw_schema',
        postgres_conn_id='postgres_etl_target_conn',
        sql="""
        -- Дропаем существующие DW tables
        DROP TABLE IF EXISTS final_fact_orders CASCADE;
        DROP TABLE IF EXISTS final_dim_product CASCADE;
        DROP TABLE IF EXISTS final_dim_customer CASCADE;
        DROP TABLE IF EXISTS final_dim_date CASCADE;

        -- Измерение: Даты, для анализа по времени (time-based analysis)
        CREATE TABLE final_dim_date (
            date_key        INTEGER PRIMARY KEY,   -- формат YYYYMMDD
            full_date       DATE        NOT NULL,
            year            INTEGER     NOT NULL,
            quarter         INTEGER     NOT NULL,
            month           INTEGER     NOT NULL,
            month_name      VARCHAR(20),
            day             INTEGER     NOT NULL,
            day_of_week     INTEGER     NOT NULL,  -- 0–6 или 1–7
            day_name        VARCHAR(20),
            is_weekend      BOOLEAN     NOT NULL
        );

        -- Измерение: Клиенты
        CREATE TABLE final_dim_customer (
            customer_key             SERIAL PRIMARY KEY,
            customer_id              TEXT        NOT NULL UNIQUE,  -- из customers.customer_id
            customer_unique_id       TEXT,
            customer_zip_code_prefix INTEGER,
            customer_city            TEXT,
            customer_state           TEXT,
            created_at               TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at               TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Измерение: Товары
        CREATE TABLE final_dim_product (
            product_key                 SERIAL PRIMARY KEY,
            product_id                  TEXT        NOT NULL UNIQUE, -- products.product_id
            product_category_name       TEXT,
            product_name_lenght         INTEGER,
            product_description_lenght  INTEGER,
            product_photos_qty          INTEGER,
            product_weight_g            INTEGER,
            product_length_cm           INTEGER,
            product_height_cm           INTEGER,
            product_width_cm            INTEGER,
            created_at                  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at                  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        -- Таблица фактов: Заказы
        CREATE TABLE final_fact_orders (
            order_item_key                 SERIAL PRIMARY KEY,   -- суррогатный ключ

            -- Натуральный ключ строки заказа
            order_id                       TEXT        NOT NULL,
            order_item_id                  INTEGER     NOT NULL,

            -- Внешние ключи на размерности
            customer_key                   INTEGER     NOT NULL
                REFERENCES final_dim_customer(customer_key),
            product_key                    INTEGER     NOT NULL
                REFERENCES final_dim_product(product_key),
            order_purchase_date_key        INTEGER     NOT NULL
                REFERENCES final_dim_date(date_key),

            -- Атрибуты заказа (из orders)
            order_status                   TEXT        NOT NULL,
            order_purchase_timestamp       TIMESTAMP   NOT NULL,
            order_approved_at              TIMESTAMP,
            order_delivered_carrier_date   TIMESTAMP,
            order_delivered_customer_date  TIMESTAMP,
            order_estimated_delivery_date  TIMESTAMP,

            -- Атрибуты строки заказа (из order_items)
            shipping_limit_date            TIMESTAMP,
            price                          NUMERIC(10,2) NOT NULL,
            freight_value                  NUMERIC(10,2) NOT NULL,

            -- Оплата (упрощённо: первый платёж по заказу)
            payment_type                   TEXT,
            payment_installments           INTEGER,
            payment_value                  NUMERIC(10,2),

            -- Тех. поля
            load_datetime                  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,

            CONSTRAINT uq_final_fact_orders_order_item
                UNIQUE (order_id, order_item_id)
        );

        -- Create indexes for better query performance
        CREATE INDEX idx_final_fact_orders_customer_key
            ON final_fact_orders (customer_key);
            
        CREATE INDEX idx_final_fact_orders_product_key
            ON final_fact_orders (product_key);

        CREATE INDEX idx_final_fact_orders_order_purchase_date_key
            ON final_fact_orders (order_purchase_date_key);

        CREATE INDEX idx_final_fact_orders_order_id
            ON final_fact_orders (order_id);
        """,
    )


    def populate_dim_date(**context):
        """Заполняем final_dim_date (диапазон покрывает)."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO final_dim_date (
            date_key,
            full_date,
            year,
            quarter,
            month,
            month_name,
            day,
            day_of_week,
            day_name,
            is_weekend
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
            d::DATE AS full_date,
            EXTRACT(YEAR FROM d)::INTEGER AS year,
            EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
            EXTRACT(MONTH FROM d)::INTEGER AS month,
            TO_CHAR(d, 'Month') AS month_name,
            EXTRACT(DAY FROM d)::INTEGER AS day,
            EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
            TO_CHAR(d, 'Day') AS day_name,
            CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM generate_series(
            '2016-01-01'::DATE,
            '2018-12-31'::DATE,
            '1 day'::INTERVAL
        ) d
        ON CONFLICT (date_key) DO NOTHING;
        """
        try:
            hook.run(sql)
            logger.info("final_dim_date заполнена")
        except Exception as e:
            logger.exception(f"Ошибка при заполнении final_dim_date: {e}")
            raise


    populate_dim_date_task = PythonOperator(
        task_id="populate_dim_date",
        python_callable=populate_dim_date,
    )


    def populate_dim_customer(**context):
        """Заполняет final_dim_customer из final_staging_customers."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO final_dim_customer (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        )
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM final_staging_customers
        ON CONFLICT (customer_id) DO UPDATE SET
            customer_unique_id       = EXCLUDED.customer_unique_id,
            customer_zip_code_prefix = EXCLUDED.customer_zip_code_prefix,
            customer_city            = EXCLUDED.customer_city,
            customer_state           = EXCLUDED.customer_state,
            updated_at               = CURRENT_TIMESTAMP;
        """
        try:
            hook.run(sql)
            logger.info("final_dim_customer заполнена")
        except Exception as e:
            logger.exception(f"Ошибка при заполнении final_dim_customer: {e}")
            raise


    populate_dim_customer_task = PythonOperator(
        task_id="populate_dim_customer",
        python_callable=populate_dim_customer,
    )


    def populate_dim_product(**context):
        """Заполняем final_dim_product из final_staging_products."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")
        sql = """
        INSERT INTO final_dim_product (
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT DISTINCT
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM final_staging_products
        ON CONFLICT (product_id) DO UPDATE SET
            product_category_name      = EXCLUDED.product_category_name,
            product_name_lenght        = EXCLUDED.product_name_lenght,
            product_description_lenght = EXCLUDED.product_description_lenght,
            product_photos_qty         = EXCLUDED.product_photos_qty,
            product_weight_g           = EXCLUDED.product_weight_g,
            product_length_cm          = EXCLUDED.product_length_cm,
            product_height_cm          = EXCLUDED.product_height_cm,
            product_width_cm           = EXCLUDED.product_width_cm,
            updated_at                 = CURRENT_TIMESTAMP;
        """
        try:
            hook.run(sql)
            logger.info("final_dim_product заполнена")
        except Exception as e:
            logger.exception(f"Ошибка при заполнении final_dim_product: {e}")
            raise


    populate_dim_product_task = PythonOperator(
        task_id="populate_dim_product",
        python_callable=populate_dim_product,
    )


    def populate_fact_orders(**context):
        """Заполняем final_fact_orders за месяц, соответствующий execution_date."""
        hook = PostgresHook(postgres_conn_id="postgres_etl_target_conn")

        # Airflow 2.x: logical_date = execution_date
        execution_date = context.get("logical_date") or context["execution_date"]
        month_start = execution_date.replace(day=1).date()
        if execution_date.month == 12:
            month_end = execution_date.replace(
                year=execution_date.year + 1, month=1, day=1
            ).date()
        else:
            month_end = execution_date.replace(
                month=execution_date.month + 1, day=1
            ).date()

        month_start_str = month_start.strftime("%Y-%m-%d")
        month_end_str = month_end.strftime("%Y-%m-%d")

        logger.info(
            f"Пересчёт final_fact_orders за период "
            f"{month_start_str} .. {month_end_str}"
        )

        sql = """
                -- Удаляем факты за месяц (идемпотентность по месяцу)
                DELETE FROM final_fact_orders
                WHERE order_purchase_timestamp::date >= %(month_start)s
                  AND order_purchase_timestamp::date < %(month_end)s;

                -- Вставляем заново все строки за этот месяц
                INSERT INTO final_fact_orders (
                    order_id,
                    order_item_id,
                    customer_key,
                    product_key,
                    order_purchase_date_key,
                    order_status,
                    order_purchase_timestamp,
                    order_approved_at,
                    order_delivered_carrier_date,
                    order_delivered_customer_date,
                    order_estimated_delivery_date,
                    shipping_limit_date,
                    price,
                    freight_value,
                    payment_type,
                    payment_installments,
                    payment_value
                )
                SELECT
                    oi.order_id,
                    oi.order_item_id,
                    dc.customer_key,
                    dp.product_key,
                    TO_CHAR(o.order_purchase_timestamp::date, 'YYYYMMDD')::INTEGER
                        AS order_purchase_date_key,
                    o.order_status,
                    o.order_purchase_timestamp,
                    o.order_approved_at,
                    o.order_delivered_carrier_date,
                    o.order_delivered_customer_date,
                    o.order_estimated_delivery_date,
                    oi.shipping_limit_date,
                    oi.price,
                    oi.freight_value,
                    pay.payment_type,
                    pay.payment_installments,
                    pay.payment_value
                FROM final_staging_order_items oi
                JOIN final_staging_orders o
                    ON o.order_id = oi.order_id
                JOIN final_dim_customer dc
                    ON dc.customer_id = o.customer_id
                JOIN final_dim_product dp
                    ON dp.product_id = oi.product_id
                LEFT JOIN (
                    SELECT DISTINCT ON (order_id)
                        order_id,
                        payment_type,
                        payment_installments,
                        payment_value
                    FROM final_staging_order_payments
                    ORDER BY order_id, payment_sequential
                ) pay
                    ON pay.order_id = o.order_id
                WHERE o.order_purchase_timestamp::date >= %(month_start)s
                  AND o.order_purchase_timestamp::date < %(month_end)s;
                """

        try:
            hook.run(
                sql,
                parameters={
                    "month_start": month_start_str,
                    "month_end": month_end_str,
                },
            )
            logger.info(
                f"final_fact_orders пересчитана за период "
                f"{month_start_str} .. {month_end_str}"
            )
        except Exception as e:
            logger.exception(f"Ошибка при заполнении final_fact_orders: {e}")
            raise


    populate_fact_orders_task = PythonOperator(
        task_id="populate_fact_orders",
        python_callable=populate_fact_orders,
    )

    # ========== TASK DEPENDENCIES ==========

    # Staging layer
    create_staging >> [load_orders, load_order_items, load_products, load_customers, load_payments]

    # DWH schema
    [load_orders, load_order_items, load_products, load_customers, load_payments] >> create_dw_schema

    # Dimensions
    create_dw_schema >> populate_dim_date_task
    [create_dw_schema, load_customers] >> populate_dim_customer_task
    [create_dw_schema, load_products] >> populate_dim_product_task

    # Fact
    [
        populate_dim_date_task,
        populate_dim_customer_task,
        populate_dim_product_task,
        load_orders,
        load_order_items,
        load_payments,
    ] >> populate_fact_orders_task