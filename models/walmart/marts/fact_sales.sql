{{ config(materialized='table')}}

SELECT 
        sales.store_id,
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
    WHERE sales.store_id IS NOT NULL 
    AND sales.dep_id IS NOT NULL 
    AND sales.sales_date IS NOT NULL