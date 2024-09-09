MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.user_purchase_summary AS target
USING (
    SELECT 
        user_id,
        user_name,
        COUNT(sale_id) AS total_purchases,
        SUM(total_revenue) AS total_spent,
        MAX(tea_name) AS favorite_tea
    FROM {{ params.db_name }}.{{ params.schema_name }}.enriched_sales
    GROUP BY user_id, user_name
) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
    UPDATE SET
        target.user_name = source.user_name,
        target.total_purchases = source.total_purchases,
        target.total_spent = source.total_spent,
        target.favorite_tea = source.favorite_tea
WHEN NOT MATCHED THEN
    INSERT (
        user_id, user_name, total_purchases, total_spent, favorite_tea
    )
    VALUES (
        source.user_id, source.user_name, source.total_purchases, source.total_spent, source.favorite_tea
    );