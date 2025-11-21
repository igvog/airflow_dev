
DROP TABLE IF EXISTS marts.customer_rfm CASCADE;

CREATE TABLE marts.customer_rfm AS
WITH rfm_base AS (
    SELECT 
        customer_id,
        -- Recency: Сколько дней прошло с последнего заказа
        EXTRACT(DAY FROM (NOW() - MAX(order_date))) AS recency,
        -- Frequency: Сколько заказов сделал
        COUNT(distinct order_id) AS frequency,
        -- Monetary: Сколько денег принес (GMV)
        SUM(gross_merchandise_value) AS monetary
    FROM marts.sales_items
    GROUP BY 1
),
rfm_scores AS (
    SELECT 
        customer_id,
        recency,
        frequency,
        monetary,
        -- Разбиваем на группы (квинтили) от 1 до 5
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score
    FROM rfm_base
)
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    -- Простая логика сегментации
    CASE 
        WHEN (r_score >= 4 AND f_score >= 4 AND m_score >= 4) THEN 'Champions'
        WHEN (f_score >= 3 AND m_score >= 3) THEN 'Loyal Customers'
        WHEN (r_score <= 2 AND recency > 90) THEN 'At Risk / Lost'
        ELSE 'Potential'
    END AS customer_segment
FROM rfm_scores;