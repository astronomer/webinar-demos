CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.revenue_by_tea_type (
    tea_type STRING PRIMARY KEY,
    total_revenue NUMBER
);