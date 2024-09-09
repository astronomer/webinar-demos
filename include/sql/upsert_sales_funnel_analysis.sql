MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.sales_funnel_analysis AS target
USING (
    SELECT 
        utm_source,
        utm_medium,
        utm_campaign,
        COUNT(sale_id) AS total_sales,
        SUM(total_revenue) AS total_revenue
    FROM {{ params.db_name }}.{{ params.schema_name }}.enriched_sales
    GROUP BY utm_source, utm_medium, utm_campaign
) AS source
ON target.utm_source = source.utm_source 
   AND target.utm_medium = source.utm_medium 
   AND target.utm_campaign = source.utm_campaign
WHEN MATCHED THEN
    UPDATE SET
        target.total_sales = source.total_sales,
        target.total_revenue = source.total_revenue
WHEN NOT MATCHED THEN
    INSERT (
        utm_source, utm_medium, utm_campaign, total_sales, total_revenue
    )
    VALUES (
        source.utm_source, source.utm_medium, source.utm_campaign, source.total_sales, source.total_revenue
    );
