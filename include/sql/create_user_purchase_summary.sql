CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.user_purchase_summary (
    user_id STRING PRIMARY KEY,
    user_name STRING,
    total_purchases INTEGER,
    total_spent NUMBER,
    favorite_tea STRING
);