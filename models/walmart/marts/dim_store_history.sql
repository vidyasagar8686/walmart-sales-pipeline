-- One row per store version (SCD Type 2), built from store_snapshot.
-- fact_sales picks the version that was valid on each sales date (point-in-time join).
WITH versions AS (
    SELECT
        dbt_scd_id AS store_version_key,
        store_id,
        store_type,
        store_size,
        dbt_valid_from,
        dbt_valid_to,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY dbt_valid_from) AS version_number
    FROM {{ ref('store_snapshot') }}
)

SELECT
    store_version_key,
    store_id,
    version_number,
    store_type,
    store_size,
    CASE
        WHEN store_size IS NULL   THEN 'Unknown'
        WHEN store_size < 50000   THEN 'Small'
        WHEN store_size < 150000  THEN 'Medium'
        ELSE 'Large'
    END AS store_size_category,
    -- the first version is valid from the beginning of time, so older sales still find a version
    CASE WHEN version_number = 1 THEN '1900-01-01'::TIMESTAMP ELSE dbt_valid_from END AS valid_from,
    -- the current version is valid until the far future
    COALESCE(dbt_valid_to, '9999-12-31'::TIMESTAMP) AS valid_to,
    dbt_valid_to IS NULL AS is_current
FROM versions
