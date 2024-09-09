CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.sales_funnel_analysis (
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    total_sales INTEGER,
    total_revenue NUMBER,
    PRIMARY KEY (utm_source, utm_medium, utm_campaign)
);
