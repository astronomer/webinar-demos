MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.top_users_by_spending AS target
USING (
    SELECT 
        user_id,
        user_name,
        total_spent
    FROM (
        SELECT 
            user_id,
            user_name,
            total_spent,
            ROW_NUMBER() OVER (ORDER BY total_spent DESC) AS rn
        FROM {{ params.db_name }}.{{ params.schema_name }}.user_purchase_summary
    ) AS ranked_users
    WHERE rn <= 10
) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN
    UPDATE SET
        target.user_name = source.user_name,
        target.total_spent = source.total_spent
WHEN NOT MATCHED THEN
    INSERT (
        user_id, user_name, total_spent
    )
    VALUES (
        source.user_id, source.user_name, source.total_spent
    );
