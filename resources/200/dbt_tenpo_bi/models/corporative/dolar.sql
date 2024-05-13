{{ config(materialized='table',  tags=["daily", "bi"]) }}

WITH 
  ----------------------------------------------------
  -->>                 VALOR DOLAR                <<--
  ----------------------------------------------------   
  dolar as (
    SELECT DISTINCT
      CASE 
       WHEN FORMAT_DATE('%Y-%m', DATE(fec_fecha_dia , "America/Santiago")) = FORMAT_DATE('%Y-%m', CURRENT_DATE("America/Santiago")) THEN 
      LAST_VALUE(vlr_valor_dolar_cierre) 
       OVER (PARTITION BY DATE(fec_fecha_dia, "America/Santiago") ORDER BY fec_fecha_dia ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
       ELSE LAST_VALUE(vlr_valor_dolar_cierre) OVER (PARTITION BY FORMAT_DATE('%Y-%m', DATE(fec_fecha_dia , "America/Santiago")) ORDER BY fec_fecha_dia ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
       END AS  valor_dolar_cierre, 
      DATE(fec_fecha_dia, "America/Santiago") as fecha,  
    FROM {{source('paypal','pay_dolar')}} ),
  dates as (
    SELECT 
      CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2014-10-29'), CURRENT_DATE(), INTERVAL 1 DAY)) dia
  ),
  cruce as (
    SELECT DISTINCT
       dates.fecha,
       valor_dolar_cierre
     FROM dates
     LEFT JOIN dolar ON dates.fecha = dolar.fecha
  ),
  dolar_final as (
    SELECT DISTINCT
       fecha,
       valor_dolar_cierre as dato_real,
       IF(valor_dolar_cierre is null, last_value(valor_dolar_cierre ignore nulls) OVER (ORDER BY fecha rows between unbounded preceding and 1 preceding), valor_dolar_cierre) valor_dolar_cierre,
    FROM cruce
)

SELECT *
FROM dolar_final