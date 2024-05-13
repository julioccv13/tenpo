{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


SELECT 
        IFNULL(B.motor,'desconocido') motor,
        A.*
FROM {{ ref('economics') }} A --FROM `tenpo-bi-prod.economics.economics` A
LEFT JOIN  {{ ref('appsflyer_users') }} B on A.user = B.customer_user_id --LEFT JOIN `tenpo-bi-prod.external.appsflyer_users` B on A.user = B.customer_user_id
WHERE linea = 'cash_in'
QUALIFY row_number() over (partition by user ORDER BY trx_timestamp ASC) = 1