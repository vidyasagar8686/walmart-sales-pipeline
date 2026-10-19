{{ config(materialized ='table') }}

SELECT 
    DISTINCT 
    Store_id,
    store_type,
    store_size,
    CASE 
        WHEN Store_size IS NULL THEN 'Unknown'
        WHEN Store_size < '50000' THEN 'Small'
        WHEN Store_size < '150000' THEN 'Medium'
    ELSE 'Large'
    END AS Store_size_category,
    current_timestamp as insert_date,
    current_timestamp as update_date
FROM {{ ref('stg_stores')}}
WHERE store_id IS NOT NULL