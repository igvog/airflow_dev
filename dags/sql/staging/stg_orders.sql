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