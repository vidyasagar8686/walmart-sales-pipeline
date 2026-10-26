-- Fails if any file differs between S3 and RAW, in row count or in content.
-- Content = HASH_AGG over the data columns, calculated the same way on both sides.
-- Catches: files never loaded, partly loaded, loaded twice, deleted from S3,
-- and files overwritten in S3 under the same name (Snowpipe doesn't reload them).
WITH s3_files AS (
    SELECT metadata$filename AS file_name,
           COUNT(*)          AS s3_rows,
           HASH_AGG($1, $2, $3) AS s3_hash
    FROM @WALMART.RAW.WALMART_S3_STAGE
        (file_format => 'WALMART.RAW.CSV_FORMAT', pattern => '.*stores.*[.]csv')
    GROUP BY 1

    UNION ALL
    SELECT metadata$filename, COUNT(*), HASH_AGG($1, $2, $3, $4, $5)
    FROM @WALMART.RAW.WALMART_S3_STAGE
        (file_format => 'WALMART.RAW.CSV_FORMAT', pattern => '.*department.*[.]csv')
    GROUP BY 1

    UNION ALL
    SELECT metadata$filename, COUNT(*),
           HASH_AGG($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    FROM @WALMART.RAW.WALMART_S3_STAGE
        (file_format => 'WALMART.RAW.CSV_FORMAT', pattern => '.*fact.*[.]csv')
    GROUP BY 1
),

raw_files AS (
    SELECT file_name, COUNT(*) AS raw_rows,
           HASH_AGG(store, type, size) AS raw_hash
    FROM {{ source('walmart_raw', 'stores') }}
    GROUP BY 1

    UNION ALL
    SELECT file_name, COUNT(*),
           HASH_AGG(store, dept, date, weekly_sales, isholiday)
    FROM {{ source('walmart_raw', 'department') }}
    GROUP BY 1

    UNION ALL
    SELECT file_name, COUNT(*),
           HASH_AGG(store, date, temperature, fuel_price, markdown1, markdown2,
                    markdown3, markdown4, markdown5, cpi, unemployment, isholiday)
    FROM {{ source('walmart_raw', 'fact') }}
    GROUP BY 1
)

SELECT COALESCE(s.file_name, r.file_name) AS file_name,
       COALESCE(s.s3_rows, 0)  AS s3_rows,
       COALESCE(r.raw_rows, 0) AS raw_rows,
       CASE WHEN COALESCE(s.s3_rows, 0) <> COALESCE(r.raw_rows, 0) THEN 'ROW COUNT'
            ELSE 'CONTENT' END AS mismatch_type
FROM s3_files s
FULL OUTER JOIN raw_files r
       ON s.file_name = r.file_name
WHERE COALESCE(s.s3_rows, 0) <> COALESCE(r.raw_rows, 0)
   OR s.s3_hash IS DISTINCT FROM r.raw_hash
