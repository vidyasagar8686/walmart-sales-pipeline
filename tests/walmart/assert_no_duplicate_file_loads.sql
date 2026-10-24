-- Fails if any file was loaded into RAW more than once
-- (for example after a pipe was recreated and its load history reset).
-- Each load of a file gets its own LOADED_AT, so more than one value = a double load.
WITH loads AS (
    SELECT 'stores' AS feed, file_name, COUNT(DISTINCT loaded_at) AS load_count, COUNT(*) AS row_count
    FROM {{ source('walmart_raw', 'stores') }}
    GROUP BY file_name
    UNION ALL
    SELECT 'department', file_name, COUNT(DISTINCT loaded_at), COUNT(*)
    FROM {{ source('walmart_raw', 'department') }}
    GROUP BY file_name
    UNION ALL
    SELECT 'fact', file_name, COUNT(DISTINCT loaded_at), COUNT(*)
    FROM {{ source('walmart_raw', 'fact') }}
    GROUP BY file_name
)
SELECT *
FROM loads
WHERE load_count > 1
   OR file_name IS NULL     -- a row with no lineage can't be traced
