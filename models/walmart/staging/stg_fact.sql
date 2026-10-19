WITH Source AS (
    SELECT * FROM {{ source('walmart_raw','fact') }}
),

fact_renamed AS (
    SELECT 
        try_to_number(store) AS store_id,
        try_to_date(date) AS sales_date,
        try_to_decimal(temperature, 6, 2) AS temperature,
        try_to_decimal(fuel_price, 6, 3) AS fuel_price,
        try_to_decimal(Markdown1, 12, 2) AS markdown1,
        try_to_decimal(Markdown2, 12, 2) AS markdown2,
        try_to_decimal(Markdown3, 12, 2) AS markdown3,
        try_to_decimal(Markdown4, 12, 2) AS markdown4,
        try_to_decimal(Markdown5, 12, 2) AS markdown5,
        try_to_decimal(CPI, 18, 7) AS cpi,
        try_to_decimal(unemployment, 6, 3) AS unemployment,
        try_to_boolean(Isholiday) AS is_holiday
    FROM Source
)

SELECT * FROM fact_renamed