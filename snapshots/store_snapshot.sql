{% snapshot store_snapshot %}

{{
    config(
        database='WALMART',
        schema='MART',
        unique_key='store_id',
        strategy='check',
        check_cols=['store_type', 'store_size']
    )
}}

SELECT
    store_id,
    store_type,
    store_size
FROM {{ ref('stg_stores') }}

{% endsnapshot %}
