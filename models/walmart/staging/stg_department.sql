with 

source as (

    select * from {{ ref('department_checked') }}
    WHERE row_status = 'CLEAN'

),

renamed as (

    select
        try_to_number(store) AS store_id,
        try_to_number(dept) AS dep_id,
        try_to_date(date) AS sales_date,
        try_to_decimal(Weekly_sales, 12, 2) AS Weekly_sales,
        try_to_boolean(Isholiday) AS Is_holiday

    from source

)

select * from renamed