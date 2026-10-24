-- Fails if any file in S3 raw/ breaks the naming rule.
-- "Newest file wins" orders by file name, so the name must carry the
-- extract date and a sequence number:  <feed>_<YYYYMMDD>_<n>.csv
-- The three original full extracts (stores.csv, department.csv, fact.csv) are allowed.
-- This also catches files the pipes would silently ignore (e.g. Department_Oct.csv).
WITH staged_files AS (
    SELECT DISTINCT metadata$filename AS file_name
    FROM @WALMART.RAW.WALMART_S3_STAGE
        (file_format => 'WALMART.RAW.LINE_FORMAT', pattern => '.*')
    WHERE metadata$file_row_number = 1
)
SELECT file_name
FROM staged_files
WHERE NOT REGEXP_LIKE(
    file_name,
    'raw/(stores|department|fact)(_[0-9]{8}_[0-9]+)?[.]csv'
)
