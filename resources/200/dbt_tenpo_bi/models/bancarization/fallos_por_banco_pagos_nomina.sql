{{ 
  config(
    materialized='table', 
  ) 
}}

WITH trx AS (

SELECT
    banco,
    banco_tienda_origen,
    count(*) transacciones
FROM {{ ref('pagos_por_nomina') }}
WHERE status = 'EXECUTED'
group by 1,2 order by 2 desc

),


fallos AS (
  SELECT
    ifo,
    count(*) fallos
FROM {{ ref('pagos_por_nomina_fallidos') }}
group by 1 order by 2 desc
)

SELECT
    trx.banco_tienda_origen,
    trx.transacciones,
    IFNULL(fallos.fallos,0) fallos
FROM trx
LEFT JOIN fallos ON trx.banco = fallos.ifo
ORDER BY 2 DESC
