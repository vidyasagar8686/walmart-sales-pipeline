WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ source('walmart_raw', 'department') }}) AS bronze_rows,
        (SELECT COUNT(*) FROM {{ ref('stg_department') }}) AS silver_clean_rows,
        (SELECT COUNT(*) FROM {{ ref('quarantine_department') }}) AS quarantined_rows,
        (SELECT COUNT(*) FROM {{ ref('department_checked') }}
          WHERE row_status = 'SUPERSEDED' AND error_reason IS NULL) AS superseded_clean_rows,
        (SELECT COUNT(*) FROM {{ ref('fact_sales') }}) AS gold_fact_rows
)
SELECT * FROM counts
WHERE bronze_rows <> silver_clean_rows + quarantined_rows + superseded_clean_rows
   OR gold_fact_rows <> silver_clean_rows
