{{ config(materialized='table')}}

SELECT 
        sales.store_id,
        hist.store_version_key,   -- store version valid on the sales date (as-was reporting)
        sales.dep_id,
        sales.sales_date,
        sales.weekly_sales,
        CASE 
            WHEN sales.weekly_sales > 0 THEN sales.weekly_sales ELSE 0 END AS positive_sales,
        CASE
            WHEN sales.weekly_sales < 0 THEN -sales.weekly_sales ELSE 0 END AS net_return_amount,
        sales.is_holiday,
        features.temperature,
        features.fuel_price,
        features.markdown1,
        features.markdown2,
        features.markdown3,
        features.markdown4,
        features.markdown5,
        features.cpi,
        features.unemployment,
        current_timestamp() AS insert_date,
        current_timestamp() AS update_date
    FROM {{ref('stg_department')}} sales 
    LEFT JOIN {{ref('stg_fact')}} features
           ON sales.store_id = features.store_id AND sales.sales_date = features.sales_date
    -- point-in-time join: date conditions in ON, so a sale without a version stays (and the not_null test fails)
    LEFT JOIN {{ ref('dim_store_history') }} hist
           ON sales.store_id    = hist.store_id
          AND sales.sales_date >= hist.valid_from
          AND sales.sales_date <  hist.valid_to
    WHERE sales.store_id IS NOT NULL 
    AND sales.dep_id IS NOT NULL 
    AND sales.sales_date IS NOT NULL
