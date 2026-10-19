{{ config(severity='warn') }}

SELECT * FROM {{ ref('quarantine_department')}}