CREATE TABLE IF NOT EXISTS 
{{ params.db_name }}.{{ params.schema_name }}.sales (
    sale_id STRING PRIMARY KEY,
    user_id STRING,
    tea_id STRING,
    utm_id STRING,
    quantity INTEGER,
    sale_date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES {{ params.db_name }}.{{ params.schema_name }}.users(user_id),
    FOREIGN KEY (tea_id) REFERENCES {{ params.db_name }}.{{ params.schema_name }}.teas(tea_id),
    FOREIGN KEY (utm_id) REFERENCES {{ params.db_name }}.{{ params.schema_name }}.utms(utm_id)
);
