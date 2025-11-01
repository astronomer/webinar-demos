CREATE OR REPLACE TEMPORARY TABLE teas_temp AS
WITH teas_numbered AS (
    SELECT
        TEA_ID,
        TEA_NAME,
        TEA_TYPE,
        PRICE,
        UPDATED_AT,
        ROW_NUMBER() OVER (PARTITION BY TEA_ID ORDER BY UPDATED_AT DESC) AS row_num
    FROM teas
)
SELECT
    TEA_ID,
    TEA_NAME,
    TEA_TYPE,
    PRICE,
    UPDATED_AT
FROM teas_numbered
WHERE row_num = 1;

TRUNCATE TABLE teas;
INSERT INTO teas SELECT * FROM teas_temp;
