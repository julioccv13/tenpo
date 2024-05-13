{{ 
  config(
    materialized='table', 
  ) 
}}

WITH data AS 
(SELECT 
    date(ts_trx) fecha,
    cico_banco_tienda_destino,
    count(distinct id_trx) transacciones,
FROM {{ ref('cca_cico_tef') }}
WHERE tipo_trx = 'cashout'
AND cico_banco_tienda_destino	not like '%TENPO%'
GROUP BY 1,2 ORDER BY 1 DESC, 2 DESC
)

SELECT
    *,
    sum(transacciones) over (partition by fecha) sum_daily,
    round( safe_divide(transacciones,sum(transacciones) over (partition by fecha)),2) as ratio
FROM data
order by fecha desc
