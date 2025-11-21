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