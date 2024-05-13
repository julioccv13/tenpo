{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


WITH hooks AS 

(SELECT 
        fecha,
        'hooks' as source,
        SUM(monto_usd) monto
--FROM `tenpo-bi-prod.marketing.hook_first_transaction`
FROM {{ref('hook_first_transaction')}}
GROUP BY 1,2


UNION ALL 

SELECT 
    fecha,
    'hooks' as source,
    SUM(monto_usd) monto
--FROM `tenpo-bi-prod.marketing.hook_first_cashin` 
FROM {{ref('hook_first_cashin')}}
GROUP BY 1,2

UNION ALL 

SELECT 
    date as fecha,
    'paid' as source,
    ROUND(SUM(total_cost)) as monto
FROM `tenpo-bi-prod.marketing.paid_media_cost_usd`
GROUP BY 1,2
ORDER BY 1 DESC


)

SELECT 
    fecha,
    source,
    SUM(monto) monto
FROM hooks
GROUP BY 1,2
ORDER BY 1 DESC
