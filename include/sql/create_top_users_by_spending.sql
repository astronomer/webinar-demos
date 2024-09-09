CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.top_users_by_spending (
    user_id STRING PRIMARY KEY,
    user_name STRING,
    total_spent NUMBER
);
