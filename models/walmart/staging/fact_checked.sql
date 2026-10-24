WITH source AS (
    SELECT * FROM {{ source('walmart_raw', 'fact') }}
),

ranked AS (
    SELECT
        *,
        -- Latest file wins for the same store and week
        ROW_NUMBER() OVER (
            PARTITION BY TRY_TO_NUMBER(store), TRY_TO_DATE(date)
            ORDER BY file_name DESC, file_row_number DESC
        ) AS key_rank
    FROM source
),

checked AS (
    SELECT
        r.*,
        CASE
            WHEN r.store IS NULL OR TRY_TO_NUMBER(r.store) IS NULL THEN 'Store is missing or invalid'
            WHEN r.date IS NULL OR TRY_TO_DATE(r.date) IS NULL THEN 'Date is Invalid'
            WHEN r.temperature IS NOT NULL AND TRY_TO_DECIMAL(r.temperature, 6, 2) IS NULL THEN 'Temperature is not number'
            WHEN r.fuel_price IS NOT NULL AND TRY_TO_DECIMAL(r.fuel_price, 6, 3) IS NULL THEN 'Fuel_price is not number'
            WHEN (r.markdown1 IS NOT NULL AND TRY_TO_DECIMAL(r.markdown1, 12, 2) IS NULL)
              OR (r.markdown2 IS NOT NULL AND TRY_TO_DECIMAL(r.markdown2, 12, 2) IS NULL)
              OR (r.markdown3 IS NOT NULL AND TRY_TO_DECIMAL(r.markdown3, 12, 2) IS NULL)
              OR (r.markdown4 IS NOT NULL AND TRY_TO_DECIMAL(r.markdown4, 12, 2) IS NULL)
              OR (r.markdown5 IS NOT NULL AND TRY_TO_DECIMAL(r.markdown5, 12, 2) IS NULL) THEN 'Markdown is not number'
            WHEN r.cpi IS NOT NULL AND TRY_TO_DECIMAL(r.cpi, 18, 7) IS NULL THEN 'CPI is not number'
            WHEN r.unemployment IS NOT NULL AND TRY_TO_DECIMAL(r.unemployment, 6, 3) IS NULL THEN 'Unemployment is not number'
            WHEN r.isholiday IS NULL OR UPPER(r.isholiday) NOT IN ('TRUE', 'FALSE') THEN 'Isholiday is Invalid'
        END AS error_reason,
        -- Only rows with a valid key can be superseded by a newer file
        (r.key_rank > 1
         AND TRY_TO_NUMBER(r.store) IS NOT NULL
         AND TRY_TO_DATE(r.date) IS NOT NULL) AS is_superseded,
        res.resolved_by_file,
        res.resolved_by_row,
        res.resolution_note
    FROM ranked r
    LEFT JOIN {{ ref('quarantine_resolutions') }} res
           ON r.file_name = res.file_name
          AND r.file_row_number = res.file_row_number
)

SELECT
    *,
    CASE
        WHEN is_superseded THEN 'SUPERSEDED'
        WHEN error_reason IS NULL THEN 'CLEAN'
        WHEN resolved_by_file IS NOT NULL THEN 'RESOLVED'
        ELSE 'OPEN'
    END AS row_status
FROM checked
