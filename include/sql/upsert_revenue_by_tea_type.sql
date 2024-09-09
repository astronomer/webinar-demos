MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.revenue_by_tea_type AS target
USING (
    SELECT 
        tea_type,
        SUM(total_revenue) AS total_revenue
    FROM {{ params.db_name }}.{{ params.schema_name }}.enriched_sales
    GROUP BY tea_type
) AS source
ON target.tea_type = source.tea_type
WHEN MATCHED THEN
    UPDATE SET
        target.total_revenue = source.total_revenue
WHEN NOT MATCHED THEN
    INSERT (
        tea_type, total_revenue
    )
    VALUES (
        source.tea_type, source.total_revenue
    );