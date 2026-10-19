{{ config(materialized='table', schema='QUARANTINE') }}

select
    *,
    current_timestamp() as quarantined_at
from {{ ref('department_checked') }}
where error_reason is not null