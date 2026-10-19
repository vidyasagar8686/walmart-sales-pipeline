{{ config(severity='error', store_failures=true) }}

{% set today = run_started_at.strftime("%Y%m%d") %}
{% set yesterday = (run_started_at - modules.datetime.timedelta(days=1)).strftime("%Y%m%d") %}

with actual as (
    select
        metadata$filename as file_name,
        trim($1)          as actual_header
    from @WALMART.RAW.WALMART_S3_STAGE
        (file_format => 'WALMART.RAW.LINE_FORMAT',
         pattern     => '.*_({{ today }}|{{ yesterday }})_.*[.]csv')
    where metadata$file_row_number = 1
),

checked as (
    select
        a.file_name,
        regexp_substr(split_part(a.file_name, '/', -1), '^[a-z]+') as file_type,
        a.actual_header,
        c.expected_header
    from actual a
    left join {{ ref('file_header_contract') }} c
           on c.file_type = regexp_substr(split_part(a.file_name, '/', -1), '^[a-z]+')
          and c.is_active
)

-- Return only the PROBLEM files
select *
from checked
where expected_header is null
   or upper(replace(actual_header, ' ', ''))
   <> upper(replace(expected_header, ' ', ''))