{{ config(materialized='ephemeral',  tags=["daily", "bi"]) }}

SELECT
  fecha AS dia,
  DATE_TRUNC(fecha, ISOWEEK) semana,
  DATE_TRUNC(fecha, MONTH) mes,
  monto gpv_clp,
  monto/valor_dolar_cierre as gpv_usd,
  CASE WHEN nombre like "%Devolución%" then null else trx_id end  as trx,
  CASE WHEN nombre like "%Devolución%" then null else user end as usr,
  linea,
  'app' as canal,
  CASE 
   WHEN linea = 'paypal' THEN 'retiro' 
   ELSE 'generico' 
   END AS tipo_usuario,
FROM {{ ref('economics') }} --`tenpo-bi-prod`.`economics`.`economics` 
JOIN {{ ref('dolar') }}  USING(fecha)  --`tenpo-bi-prod`.`corporative`.`dolar`   USING(fecha)
WHERE 
  linea in ('mastercard', 'mastercard_physical' ,'utility_payments', 'top_ups', 'p2p','paypal','crossborder', 'cash_in_savings')