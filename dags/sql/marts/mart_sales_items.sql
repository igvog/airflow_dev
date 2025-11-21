DROP TABLE IF EXISTS marts.sales_items CASCADE;

CREATE TABLE marts.sales_items AS
SELECT
    -- Ключи
    i.order_id,
    i.product_id,
    i.seller_id,
    o.customer_id,

    -- Метрики
    i.price,
    i.freight_value,
    -- Gross Merchandise Value (GMV) = Цена + Стоимость доставки
    (i.price + i.freight_value) AS gross_merchandise_value,

    -- Даты и Статус
    o.order_date,
    o.delivered_at,
    o.order_status,
    
    -- Метрика времени: Разница между доставкой и покупкой (в днях)
    EXTRACT(DAY FROM (o.delivered_at - o.order_date)) AS delivery_time_days
    
FROM staging.items i
INNER JOIN staging.orders o 
    ON i.order_id = o.order_id

-- Фильтруем только доставленные или отгруженные заказы
WHERE o.order_status IN ('delivered', 'shipped');