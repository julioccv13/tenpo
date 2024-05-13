{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


WITH first_transaction AS 
(
    SELECT user, 
            DATE(trx_timestamp,"America/Santiago") as fecha,
            monto, 
            ROW_NUMBER() OVER (PARTITION BY user ORDER BY trx_timestamp ASC) AS row 
    --FROM {{ref('economics')}}
    FROM {{ ref('economics') }}
where linea in ('mastercard','utility_payments','top_ups')

)

SELECT DISTINCT
        fecha,
        user, 
        monto
FROM first_transaction
WHERE ROW = 1
