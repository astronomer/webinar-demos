CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.enriched_sales (
    sale_id STRING,
    user_id STRING,
    user_name STRING,
    tea_name STRING,
    tea_type STRING,
    quantity INT,
    sale_date TIMESTAMP,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_term STRING,
    utm_content STRING,
    total_revenue NUMBER,
    CONSTRAINT unique_sale_id UNIQUE (sale_id)
) AS
SELECT 
    s.sale_id,
    s.user_id,
    u.user_name,
    t.tea_name,
    t.tea_type,
    s.quantity,
    s.sale_date,
    up.utm_source,
    up.utm_medium,
    up.utm_campaign,
    up.utm_term,
    up.utm_content,
    t.price * s.quantity AS total_revenue
FROM {{ params.db_name }}.{{ params.schema_name }}.sales s
JOIN {{ params.db_name }}.{{ params.schema_name }}.users u ON s.user_id = u.user_id
JOIN {{ params.db_name }}.{{ params.schema_name }}.teas t ON s.tea_id = t.tea_id
JOIN {{ params.db_name }}.{{ params.schema_name }}.utms up ON s.utm_id = up.utm_id;
