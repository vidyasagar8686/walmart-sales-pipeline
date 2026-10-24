{{ config(severity='error', store_failures=true) }}

-- Check EVERY file's header against the contract version
-- that was valid on the day the file arrived in S3
WITH actual AS (
    SELECT
        metadata$filename           AS file_name,
        metadata$file_last_modified AS file_last_modified,
        TRIM($1)                    AS actual_header
    FROM @WALMART.RAW.WALMART_S3_STAGE
        (file_format => 'WALMART.RAW.LINE_FORMAT',
         pattern     => '.*[.]csv')
    WHERE metadata$file_row_number = 1
),

checked AS (
    SELECT
        a.file_name,
        a.file_last_modified,
        REGEXP_SUBSTR(SPLIT_PART(a.file_name, '/', -1), '^[a-z]+') AS file_type,
        a.actual_header,
        c.contract_version,
        c.expected_header
    FROM actual a
    LEFT JOIN {{ ref('file_header_contract') }} c
           ON c.file_type = REGEXP_SUBSTR(SPLIT_PART(a.file_name, '/', -1), '^[a-z]+')
          AND a.file_last_modified::DATE >= TRY_TO_DATE(TO_VARCHAR(c.valid_from))
          AND (c.valid_to IS NULL
               OR a.file_last_modified::DATE < TRY_TO_DATE(TO_VARCHAR(c.valid_to)))
)

-- Return only the PROBLEM files
SELECT *
FROM checked
WHERE expected_header IS NULL
   OR UPPER(REPLACE(actual_header, ' ', ''))
   <> UPPER(REPLACE(expected_header, ' ', ''))
