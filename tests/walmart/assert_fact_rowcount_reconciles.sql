-- Every Bronze fact row is CLEAN (in Silver), quarantined, or superseded, exactly once
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ source('walmart_raw', 'fact') }}) AS bronze_rows,
        (SELECT COUNT(*) FROM {{ ref('stg_fact') }}) AS silver_clean_rows,
        (SELECT COUNT(*) FROM {{ ref('quarantine_fact') }}) AS quarantined_rows,
        (SELECT COUNT(*) FROM {{ ref('fact_checked') }}
          WHERE row_status = 'SUPERSEDED' AND error_reason IS NULL) AS superseded_clean_rows
)
SELECT * FROM counts
WHERE bronze_rows <> silver_clean_rows + quarantined_rows + superseded_clean_rows
