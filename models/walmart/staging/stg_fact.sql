WITH source AS (
    SELECT * FROM {{ ref('fact_checked') }}
    WHERE row_status = 'CLEAN'
),

fact_renamed AS (
    SELECT
        try_to_number(store) AS store_id,
        try_to_date(date) AS sales_date,
        try_to_decimal(temperature, 6, 2) AS temperature,
        try_to_decimal(fuel_price, 6, 3) AS fuel_price,
        try_to_decimal(markdown1, 12, 2) AS markdown1,
        try_to_decimal(markdown2, 12, 2) AS markdown2,
        try_to_decimal(markdown3, 12, 2) AS markdown3,
        try_to_decimal(markdown4, 12, 2) AS markdown4,
        try_to_decimal(markdown5, 12, 2) AS markdown5,
        try_to_decimal(cpi, 18, 7) AS cpi,
        try_to_decimal(unemployment, 6, 3) AS unemployment,
        try_to_boolean(isholiday) AS is_holiday,
        file_name,
        file_row_number
    FROM source
)

SELECT * FROM fact_renamed
