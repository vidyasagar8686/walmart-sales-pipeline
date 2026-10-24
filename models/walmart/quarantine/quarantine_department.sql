{{ config(materialized='table', schema='QUARANTINE') }}

SELECT
    file_name,
    file_row_number,
    store,
    dept,
    date,
    weekly_sales,
    isholiday,
    error_reason,
    row_status       AS quarantine_status,   -- OPEN, RESOLVED or SUPERSEDED
    resolved_by_file,
    resolved_by_row,
    resolution_note,
    loaded_at        AS first_seen_at        -- when the bad row arrived
FROM {{ ref('department_checked') }}
WHERE error_reason IS NOT NULL
