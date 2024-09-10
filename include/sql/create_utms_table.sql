CREATE TABLE IF NOT EXISTS 
{{ params.db_name }}.{{ params.schema_name }}.utms (
    utm_id STRING PRIMARY KEY,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_term STRING,
    utm_content STRING,
    updated_at TIMESTAMP
);
