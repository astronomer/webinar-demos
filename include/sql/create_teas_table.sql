CREATE TABLE IF NOT EXISTS 
{{ params.db_name }}.{{ params.schema_name }}.teas (
    tea_id STRING PRIMARY KEY,
    tea_name STRING,
    tea_type STRING,
    price FLOAT,
    updated_at TIMESTAMP
);
