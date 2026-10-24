{{ config(materialized='table', schema='QUARANTINE') }}

SELECT
    file_name,
    file_row_number,
    store,
    date,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    unemployment,
    isholiday,
    error_reason,
    row_status       AS quarantine_status,   -- OPEN, RESOLVED or SUPERSEDED
    resolved_by_file,
    resolved_by_row,
    resolution_note,
    loaded_at        AS first_seen_at        -- when the bad row arrived
FROM {{ ref('fact_checked') }}
WHERE error_reason IS NOT NULL
