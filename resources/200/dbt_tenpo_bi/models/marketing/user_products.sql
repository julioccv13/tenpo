{{ 
  config(
    materialized='table',
  ) 
}}

SELECT 
email
,COALESCE(MAX(CASE WHEN linea = 'mastercard' then 1 else null end), 0) as tiene_mastercard
,COALESCE(MAX(CASE WHEN linea = 'utility_payments' then 1 else null end), 0) as tiene_utility_payments
,COALESCE(MAX(CASE WHEN linea = 'top_ups' then 1 else null end), 0) as tiene_topups
FROM {{ ref('economics') }}
group by 1
