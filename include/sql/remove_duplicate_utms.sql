CREATE OR REPLACE TEMPORARY TABLE temp_utms AS
WITH utms_numbered AS (
    SELECT
        UTM_ID,
        UTM_SOURCE,
        UTM_MEDIUM,
        UTM_CAMPAIGN,
        UTM_TERM,
        UTM_CONTENT,
        UPDATED_AT,
        ROW_NUMBER() OVER (PARTITION BY UTM_ID ORDER BY UPDATED_AT DESC) AS row_num
    FROM utms
)
SELECT
    UTM_ID,
    UTM_SOURCE,
    UTM_MEDIUM,
    UTM_CAMPAIGN,
    UTM_TERM,
    UTM_CONTENT,
    UPDATED_AT
FROM utms_numbered
WHERE row_num = 1;

TRUNCATE TABLE utms;
INSERT INTO utms SELECT * FROM temp_utms;
