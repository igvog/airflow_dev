

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk        BIGSERIAL PRIMARY KEY,
    customer_id        VARCHAR(50) UNIQUE,
    customer_unique_id VARCHAR(50),
    customer_city      TEXT,
    customer_state     TEXT
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id           BIGINT PRIMARY KEY,
    category_name         TEXT,
    category_name_english TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk                    BIGSERIAL PRIMARY KEY,
    product_id                    VARCHAR(50) UNIQUE,
    product_category_id           BIGINT REFERENCES dim_category(category_id),
    category_name                 TEXT,
    category_name_english         TEXT,
    product_weight_g              NUMERIC(18,4),
    product_length_cm             NUMERIC(18,4),
    product_height_cm             NUMERIC(18,4),
    product_width_cm              NUMERIC(18,4)
);

CREATE TABLE IF NOT EXISTS fact_orders (
    order_sk                      BIGSERIAL PRIMARY KEY,
    order_id                      VARCHAR(50) UNIQUE,
    customer_sk                   BIGINT REFERENCES dim_customer(customer_sk),
    order_purchase_date           DATE,
    order_status                  TEXT,
    order_approved_at             TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    load_type                     TEXT,
    inserted_at                   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_order_id
    ON fact_orders(order_id);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_sk       BIGSERIAL PRIMARY KEY,
    order_id            VARCHAR(50),
    product_sk          BIGINT REFERENCES dim_product(product_sk),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(18,4),
    freight_value       NUMERIC(18,4),
    load_type           TEXT,
    inserted_at         TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_order_items_order_id
    ON fact_order_items(order_id);

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_sk           BIGSERIAL PRIMARY KEY,
    order_id             VARCHAR(50),
    payment_sequential   BIGINT,
    payment_type         TEXT,
    payment_installments BIGINT,
    payment_value        NUMERIC(18,4),
    load_type            TEXT,
    inserted_at          TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_payments_order_id
    ON fact_payments(order_id);