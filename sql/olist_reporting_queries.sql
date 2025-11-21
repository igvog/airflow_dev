-- Olist reporting/demo queries (Postgres)
-- Connection params (docker-compose):
--   host: localhost
--   port: 5433
--   db:   etl_db
--   user: etl_user
--   pass: etl_pass

-- 1) Row counts and freshness
SELECT 'fact_order_items' AS table, COUNT(*) AS rows FROM fact_order_items
UNION ALL
SELECT 'fact_payments', COUNT(*) FROM fact_payments
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM dim_customers
UNION ALL
SELECT 'dim_sellers', COUNT(*) FROM dim_sellers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products;

SELECT MIN(load_date) AS min_load_date, MAX(load_date) AS max_load_date FROM fact_order_items;

  SELECT dd.full_date, foi.order_status, COUNT(*) AS cnt
  FROM fact_order_items foi
  JOIN dim_dates dd ON foi.order_purchase_date_key = dd.date_key
  GROUP BY dd.full_date, foi.order_status
  ORDER BY dd.full_date, foi.order_status;

-- 2) Orders by status
SELECT order_status, COUNT(*) AS cnt
FROM fact_order_items
GROUP BY order_status
ORDER BY cnt DESC;

-- 3) Revenue and freight by month (purchase date)
SELECT
  dd.year,
  dd.month,
  dd.month_name,
  SUM(foi.price) AS gross_revenue,
  SUM(foi.freight_value) AS freight
FROM fact_order_items foi
JOIN dim_dates dd ON foi.order_purchase_date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- 4) Payments split by type
SELECT payment_type, COUNT(*) AS cnt, SUM(payment_value) AS amount
FROM fact_payments
GROUP BY payment_type
ORDER BY amount DESC;

-- 5) Top categories by revenue
SELECT
  dp.product_category_name_english AS category,
  SUM(foi.price) AS revenue
FROM fact_order_items foi
LEFT JOIN dim_products dp ON foi.product_key = dp.product_key
GROUP BY category
ORDER BY revenue DESC
LIMIT 10;

-- 6) Geo: revenue by seller state
SELECT
  ds.state AS seller_state,
  COUNT(*) AS items,
  SUM(foi.price) AS revenue
FROM fact_order_items foi
JOIN dim_sellers ds ON foi.seller_key = ds.seller_key
GROUP BY seller_state
ORDER BY revenue DESC;

-- 7) Service level: delivery delay vs estimated date
SELECT
  CASE
    WHEN dd_delivered.full_date <= dd_est.full_date THEN 'on_time'
    WHEN dd_delivered.full_date IS NULL THEN 'missing'
    ELSE 'late'
  END AS delivery_status,
  COUNT(*) AS cnt
FROM fact_order_items foi
LEFT JOIN dim_dates dd_delivered ON foi.delivered_customer_date_key = dd_delivered.date_key
LEFT JOIN dim_dates dd_est ON foi.estimated_delivery_date_key = dd_est.date_key
GROUP BY delivery_status;

-- 8) Customer order frequency (simple R from RFM)
WITH customer_orders AS (
    SELECT dc.customer_unique_id, COUNT(DISTINCT foi.order_id) AS orders
    FROM fact_order_items foi
    JOIN dim_customers dc ON foi.customer_key = dc.customer_key
    GROUP BY dc.customer_unique_id
  )
  SELECT
    CASE
      WHEN orders = 1 THEN '1 order'
      WHEN orders BETWEEN 2 AND 3 THEN '2-3 orders'
      WHEN orders BETWEEN 4 AND 5 THEN '4-5 orders'
      ELSE '6+ orders'
    END AS bucket,
    COUNT(*) AS customers
  FROM customer_orders
  GROUP BY bucket
  ORDER BY customers DESC;

-- 9) Average basket (items per order and revenue per order)
WITH order_rollup AS (
  SELECT
    order_id,
    SUM(price) AS order_revenue,
    SUM(freight_value) AS order_freight,
    COUNT(*) AS items
  FROM fact_order_items
  GROUP BY order_id
)
SELECT
  AVG(items) AS avg_items_per_order,
  AVG(order_revenue) AS avg_revenue_per_order,
  AVG(order_freight) AS avg_freight_per_order
FROM order_rollup;

  SELECT COUNT(*) FROM fact_order_items;
-- 10) Payment vs order totals (sanity join)
WITH order_revenue AS (
  SELECT order_id, SUM(price + freight_value) AS total_value
  FROM fact_order_items
  GROUP BY order_id
),
payment_total AS (
  SELECT order_id, SUM(payment_value) AS paid_value
  FROM fact_payments
  GROUP BY order_id
)
SELECT
  COUNT(*) FILTER (WHERE ABS(o.total_value - p.paid_value) <= 0.01) AS matched,
  COUNT(*) FILTER (WHERE ABS(o.total_value - p.paid_value) > 0.01) AS mismatched
FROM order_revenue o
LEFT JOIN payment_total p ON o.order_id = p.order_id;
