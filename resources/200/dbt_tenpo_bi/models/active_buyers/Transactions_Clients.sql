{{ 
  config(
    tags=["daily", "bi"],
    materialized='table',
     project=env_var('DBT_PROJECT2', 'tenpo-datalake-sandbox'),
    schema='Camadas_v2'
  ) 
}}


SELECT DISTINCT user,DATE_TRUNC(DATE(dt_trx_chile),MONTH) AS month
FROM {{ ref('economics') }}
WHERE  lower(linea) not in ('reward','pfm','saldo')
AND nombre not like "%Devolución%"