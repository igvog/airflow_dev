import os

# Путь: dags/sql/staging
BASE_DIR = "dags/sql/staging"

# Создаем папки, если их нет
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)
    print(f"📁 Создана папка: {BASE_DIR}")

# --- 1. stg_orders.sql ---
orders_sql = """
DROP TABLE IF EXISTS staging.orders CASCADE;

CREATE TABLE staging.orders AS
SELECT
    order_id,
    customer_id,
    order_status,
    -- Безопасное преобразование типов (NULL если ошибка)
    CAST(NULLIF(order_purchase_timestamp, '') AS TIMESTAMP) AS order_date,
    CAST(NULLIF(order_approved_at, '') AS TIMESTAMP) AS approved_at,
    CAST(NULLIF(order_delivered_carrier_date, '') AS TIMESTAMP) AS picked_up_at,
    CAST(NULLIF(order_delivered_customer_date, '') AS TIMESTAMP) AS delivered_at,
    CAST(NULLIF(order_estimated_delivery_date, '') AS TIMESTAMP) AS estimated_delivery_at
FROM raw.orders_dataset;
"""

# --- 2. stg_items.sql ---
items_sql = """
DROP TABLE IF EXISTS staging.items CASCADE;

CREATE TABLE staging.items AS
SELECT
    order_id,
    -- Преобразуем в числа
    CAST(order_item_id AS INTEGER) AS item_number,
    product_id,
    seller_id,
    CAST(NULLIF(shipping_limit_date, '') AS TIMESTAMP) AS shipping_limit_date,
    CAST(price AS NUMERIC(10, 2)) AS price,
    CAST(freight_value AS NUMERIC(10, 2)) AS freight_value
FROM raw.order_items_dataset;
"""

# --- 3. stg_products.sql ---
products_sql = """
DROP TABLE IF EXISTS staging.products CASCADE;

CREATE TABLE staging.products AS
SELECT
    product_id,
    -- Заменяем пустые категории
    COALESCE(product_category_name, 'unknown') AS category_name,
    CAST(NULLIF(product_name_lenght, '') AS INTEGER) AS name_length,
    CAST(NULLIF(product_description_lenght, '') AS INTEGER) AS desc_length,
    CAST(NULLIF(product_photos_qty, '') AS INTEGER) AS photos_qty,
    CAST(NULLIF(product_weight_g, '') AS INTEGER) AS weight_g
FROM raw.products_dataset;
"""

# Функция для записи файла
def write_sql(filename, content):
    path = os.path.join(BASE_DIR, filename)
    with open(path, "w") as f:
        f.write(content.strip())
    print(f"✅ Файл создан: {path}")

# Записываем файлы
write_sql("stg_orders.sql", orders_sql)
write_sql("stg_items.sql", items_sql)
write_sql("stg_products.sql", products_sql)

print("🎉 Все SQL-файлы готовы!")