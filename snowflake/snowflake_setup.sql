-- =====================================================================
-- Walmart Sales Analytics Pipeline: Snowflake setup
-- Run as ACCOUNTADMIN in a Snowflake worksheet, top to bottom.
-- Replace every <placeholder> before running.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 1. Warehouse, database and schemas
-- ---------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE;

CREATE DATABASE IF NOT EXISTS WALMART;
CREATE SCHEMA IF NOT EXISTS WALMART.RAW;
CREATE SCHEMA IF NOT EXISTS WALMART.STAGING;
CREATE SCHEMA IF NOT EXISTS WALMART.MART;
-- CONTROL, AUDIT and QUARANTINE are created automatically by dbt.


-- ---------------------------------------------------------------------
-- 2. Storage integration (secure connection to S3, no access keys)
-- ---------------------------------------------------------------------
CREATE STORAGE INTEGRATION IF NOT EXISTS WALMART_S3_INT
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<aws_account_id>:role/de-walmart-iam-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://de-walmart-bucket/raw/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- into the IAM role's trust policy in AWS.
DESC INTEGRATION WALMART_S3_INT;


-- ---------------------------------------------------------------------
-- 3. File formats
-- ---------------------------------------------------------------------
-- For loading data: splits columns, skips the header, turns NA into NULL
CREATE FILE FORMAT IF NOT EXISTS WALMART.RAW.CSV_FORMAT
  TYPE                         = CSV
  SKIP_HEADER                  = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF                      = ('NULL', 'null', 'NA', '')
  EMPTY_FIELD_AS_NULL          = TRUE;

-- For reading headers: one whole line per value, header kept (Scenario 1)
CREATE FILE FORMAT IF NOT EXISTS WALMART.RAW.LINE_FORMAT
  TYPE            = CSV
  FIELD_DELIMITER = NONE;


-- ---------------------------------------------------------------------
-- 4. External stage
-- ---------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS WALMART.RAW.WALMART_S3_STAGE
  URL                 = 's3://de-walmart-bucket/raw/'
  STORAGE_INTEGRATION = WALMART_S3_INT
  FILE_FORMAT         = WALMART.RAW.CSV_FORMAT;

-- Test the connection: should list the source files
LIST @WALMART.RAW.WALMART_S3_STAGE;


-- ---------------------------------------------------------------------
-- 5. RAW tables (all VARCHAR: no load ever fails on a bad value)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS WALMART.RAW.STORES (
    STORE      VARCHAR,
    TYPE       VARCHAR,
    SIZE       VARCHAR,
    FILE_NAME  VARCHAR,          -- lineage: which file
    LOADED_AT  TIMESTAMP_LTZ     -- lineage: when
);

CREATE TABLE IF NOT EXISTS WALMART.RAW.DEPARTMENT (
    STORE        VARCHAR,
    DEPT         VARCHAR,
    DATE         VARCHAR,
    WEEKLY_SALES VARCHAR,
    ISHOLIDAY    VARCHAR,
    FILE_NAME       VARCHAR,        -- lineage: which file
    LOADED_AT       TIMESTAMP_LTZ,  -- lineage: when
    FILE_ROW_NUMBER NUMBER          -- lineage: which line in the file
);

CREATE TABLE IF NOT EXISTS WALMART.RAW.FACT (
    STORE        VARCHAR,
    DATE         VARCHAR,
    TEMPERATURE  VARCHAR,
    FUEL_PRICE   VARCHAR,
    MARKDOWN1    VARCHAR,
    MARKDOWN2    VARCHAR,
    MARKDOWN3    VARCHAR,
    MARKDOWN4    VARCHAR,
    MARKDOWN5    VARCHAR,
    CPI          VARCHAR,
    UNEMPLOYMENT VARCHAR,
    ISHOLIDAY    VARCHAR,
    FILE_NAME       VARCHAR,        -- lineage: which file
    LOADED_AT       TIMESTAMP_LTZ,  -- lineage: when
    FILE_ROW_NUMBER NUMBER          -- lineage: which line in the file
);


-- ---------------------------------------------------------------------
-- 6. Snowpipe: auto-ingest pipes (one per file type, routed by name)
-- ---------------------------------------------------------------------
CREATE OR REPLACE PIPE WALMART.RAW.STORES_PIPE AUTO_INGEST = TRUE AS
COPY INTO WALMART.RAW.STORES (STORE, TYPE, SIZE, FILE_NAME, LOADED_AT)
FROM (
    SELECT $1, $2, $3, METADATA$FILENAME, METADATA$START_SCAN_TIME
    FROM @WALMART.RAW.WALMART_S3_STAGE
)
PATTERN = '.*stores.*[.]csv';

CREATE OR REPLACE PIPE WALMART.RAW.DEPARTMENT_PIPE AUTO_INGEST = TRUE AS
COPY INTO WALMART.RAW.DEPARTMENT (STORE, DEPT, DATE, WEEKLY_SALES, ISHOLIDAY, FILE_NAME, LOADED_AT, FILE_ROW_NUMBER)
FROM (
    SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, METADATA$START_SCAN_TIME, METADATA$FILE_ROW_NUMBER
    FROM @WALMART.RAW.WALMART_S3_STAGE
)
PATTERN = '.*department.*[.]csv';

CREATE OR REPLACE PIPE WALMART.RAW.FACT_PIPE AUTO_INGEST = TRUE AS
COPY INTO WALMART.RAW.FACT (STORE, DATE, TEMPERATURE, FUEL_PRICE, MARKDOWN1, MARKDOWN2, MARKDOWN3, MARKDOWN4, MARKDOWN5, CPI, UNEMPLOYMENT, ISHOLIDAY, FILE_NAME, LOADED_AT, FILE_ROW_NUMBER)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, METADATA$FILENAME, METADATA$START_SCAN_TIME, METADATA$FILE_ROW_NUMBER
    FROM @WALMART.RAW.WALMART_S3_STAGE
)
PATTERN = '.*fact.*[.]csv';

-- Copy notification_channel (the SQS ARN) into the S3 event notification
-- (bucket → Properties → Event notifications, prefix raw/, all object-create events)
SHOW PIPES IN SCHEMA WALMART.RAW;

-- Load files that were already in the bucket before the pipes existed
ALTER PIPE WALMART.RAW.STORES_PIPE     REFRESH;
ALTER PIPE WALMART.RAW.DEPARTMENT_PIPE REFRESH;
ALTER PIPE WALMART.RAW.FACT_PIPE       REFRESH;


-- ---------------------------------------------------------------------
-- 7. Verify
-- ---------------------------------------------------------------------
SELECT SYSTEM$PIPE_STATUS('WALMART.RAW.STORES_PIPE');   -- executionState = RUNNING

SELECT COUNT(*) AS stores_rows     FROM WALMART.RAW.STORES;      -- 45
SELECT COUNT(*) AS department_rows FROM WALMART.RAW.DEPARTMENT;  -- 421,570 for the original department.csv
SELECT COUNT(*) AS fact_rows       FROM WALMART.RAW.FACT;        -- 8,190


-- ---------------------------------------------------------------------
-- 8. Runbook: changing a pipe or a RAW table
-- Recreating a pipe RESETS its load history, so a file can load twice.
-- Run each statement on its own and check the result before the next.
-- ---------------------------------------------------------------------
-- 1. Pause the pipe (the ingestion gate then blocks the dbt job)
--    ALTER PIPE WALMART.RAW.DEPARTMENT_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
-- 2. Back up the table
--    CREATE TABLE WALMART.RAW.DEPARTMENT_BACKUP CLONE WALMART.RAW.DEPARTMENT;
-- 3. Change the table / recreate the pipe with the new COPY column list
-- 4. Reload from S3 (TRUNCATE also clears the table's load history)
--    TRUNCATE TABLE WALMART.RAW.DEPARTMENT;
--    COPY INTO WALMART.RAW.DEPARTMENT (...) FROM (SELECT ...) PATTERN = '.*department.*[.]csv';
-- 5. Reconcile the COPY result per file with the files in S3,
--    and run: dbt test --select assert_no_duplicate_file_loads assert_file_names_follow_convention
-- 6. SHOW PIPES (same notification_channel?), resume the pipe, run the job
--    ALTER PIPE WALMART.RAW.DEPARTMENT_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;
