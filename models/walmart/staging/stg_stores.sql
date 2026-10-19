with

source as (

    select * from {{ source('walmart_raw', 'stores') }}

),

renamed as (

    select
        try_to_number(store) as store_id,
        trim(type)           as store_type,
        try_to_number(size)  as store_size,
        file_name,
        loaded_at
    from source

)

select *
from renamed
qualify row_number() over (
    partition by store_id
    order by file_name desc, loaded_at desc
) = 1