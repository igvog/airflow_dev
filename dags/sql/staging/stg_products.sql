DROP TABLE IF EXISTS staging.products CASCADE;

CREATE TABLE staging.products AS
SELECT
    product_id,
    COALESCE(product_category_name, 'unknown') AS category_name,
    
    -- ВСЕ ПОЛЯ ПЕРЕВОДИМ В NUMERIC, чтобы избежать ошибки с ".0"
    CAST(NULLIF(product_name_lenght, '') AS NUMERIC) AS name_length,
    CAST(NULLIF(product_description_lenght, '') AS NUMERIC) AS desc_length,
    CAST(NULLIF(product_photos_qty, '') AS NUMERIC) AS photos_qty,
    
    CAST(NULLIF(product_weight_g, '') AS NUMERIC) AS weight_g,
    CAST(NULLIF(product_length_cm, '') AS NUMERIC) AS length_cm,
    CAST(NULLIF(product_height_cm, '') AS NUMERIC) AS height_cm,
    CAST(NULLIF(product_width_cm, '') AS NUMERIC) AS width_cm
FROM raw.products_dataset;