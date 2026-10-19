{{ config(materialized='table') }}

SELECT DISTINCT
    sales_date,
    YEAR(sales_date) AS sales_year,
    QUARTER(sales_date) AS sales_quarter,
    MONTH(sales_date) AS sales_month,
    WEEK(sales_date) AS sales_week,
    DAY(sales_date) AS sales_day,
    is_holiday,
    current_timestamp as insert_date,
    current_timestamp as update_date
FROM {{ ref('stg_department') }}
WHERE sales_date IS NOT NULL