WITH Source AS (
    SELECT * FROM {{ source('walmart_raw', 'department') }}
)
SELECT 
    *,
        CASE 
            WHEN Store IS NULL THEN 'Store is missing'
            WHEN weekly_sales IS NOT NULL AND try_to_decimal(weekly_sales, 12, 2) IS NULL THEN 'Weekly_sales is not number'
            WHEN date IS NOT NULL AND try_to_date(date) IS NULL THEN 'Date is Invalid'
            WHEN isholiday IS NULL OR upper(isholiday) NOT IN ('TRUE','FALSE') THEN 'Isholiday is Invalid'
        END AS error_reason
    FROM Source
