SELECT
    *
FROM {{ ref('fact_sales') }}
WHERE positive_sales - net_return_amount <> weekly_sales
OR positive_sales < 0
OR net_return_amount < 0