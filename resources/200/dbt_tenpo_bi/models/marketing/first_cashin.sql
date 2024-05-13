{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


WITH first_cashin AS 
(
    SELECT user,
            DATE(trx_timestamp,"America/Santiago") as fecha,
            monto, 
            ROW_NUMBER() OVER (PARTITION BY user ORDER BY trx_timestamp ASC) AS row 
    FROM {{ ref('economics') }}
WHERE linea = 'cash_in'
)

SELECT
        fecha,
        user,
        monto
FROM first_cashin
WHERE ROW = 1