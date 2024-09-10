CREATE TABLE IF NOT EXISTS 
{{ params.db_name }}.{{ params.schema_name }}.users (
    user_id STRING PRIMARY KEY,
    user_name STRING,
    date_of_birth DATE,
    sign_up_date DATE,
    updated_at TIMESTAMP
);
