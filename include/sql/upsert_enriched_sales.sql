MERGE INTO ETL_DEMO.DEV.enriched_sales AS target
USING (
    SELECT 
        *
    FROM (
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
            t.price * s.quantity AS total_revenue,
            ROW_NUMBER() OVER (PARTITION BY s.sale_id ORDER BY s.sale_date DESC) AS rn
        FROM 
            {{ params.db_name }}.{{ params.schema_name }}.sales s
        JOIN 
            {{ params.db_name }}.{{ params.schema_name }}.users u ON s.user_id = u.user_id
        JOIN 
            {{ params.db_name }}.{{ params.schema_name }}.teas t ON s.tea_id = t.tea_id
        JOIN 
            {{ params.db_name }}.{{ params.schema_name }}.utms up ON s.utm_id = up.utm_id
    ) AS subquery
    WHERE 
        rn = 1
) AS source
ON 
    target.sale_id = source.sale_id
WHEN MATCHED THEN
    UPDATE SET
        target.user_id = source.user_id,
        target.user_name = source.user_name,
        target.tea_name = source.tea_name,
        target.tea_type = source.tea_type,
        target.quantity = source.quantity,
        target.sale_date = source.sale_date,
        target.utm_source = source.utm_source,
        target.utm_medium = source.utm_medium,
        target.utm_campaign = source.utm_campaign,
        target.utm_term = source.utm_term,
        target.utm_content = source.utm_content,
        target.total_revenue = source.total_revenue
WHEN NOT MATCHED THEN
    INSERT (
        sale_id, user_id, user_name, tea_name, tea_type, quantity, sale_date,
        utm_source, utm_medium, utm_campaign, utm_term, utm_content, total_revenue
    )
    VALUES (
        source.sale_id, source.user_id, source.user_name, source.tea_name, source.tea_type, 
        source.quantity, source.sale_date, source.utm_source, source.utm_medium, source.utm_campaign, 
        source.utm_term, source.utm_content, source.total_revenue
    );
