{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}

SELECT 
        IFNULL(B.motor,'desconocido') motor,
        A.*
FROM {{ ref('economics') }} A
LEFT JOIN  {{ ref('appsflyer_users') }} B on A.user = B.customer_user_id
WHERE linea in ('mastercard','utility_payments','top_ups')
QUALIFY row_number() over (partition by user ORDER BY trx_timestamp ASC) = 1