-- fact_sales grain: one row per store, department and week
-- Returns any combination that appears more than once (0 rows = pass)
SELECT store_id, dep_id, sales_date, COUNT(*) AS row_count
FROM {{ ref('fact_sales') }}
GROUP BY store_id, dep_id, sales_date
HAVING COUNT(*) > 1
