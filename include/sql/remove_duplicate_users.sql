CREATE OR REPLACE TEMPORARY TABLE temp_users AS
WITH users_numbered AS (
    SELECT
        USER_ID,
        USER_NAME,
        DATE_OF_BIRTH,
        SIGN_UP_DATE,
        UPDATED_AT,
        ROW_NUMBER() OVER (PARTITION BY USER_ID ORDER BY UPDATED_AT DESC) AS row_num
    FROM users
)
SELECT
    USER_ID,
    USER_NAME,
    DATE_OF_BIRTH,
    SIGN_UP_DATE,
    UPDATED_AT
FROM users_numbered
WHERE row_num = 1;

TRUNCATE TABLE users;
INSERT INTO users SELECT * FROM temp_users;
