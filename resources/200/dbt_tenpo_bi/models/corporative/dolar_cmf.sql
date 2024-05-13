{{ config(materialized='table',  tags=["daily", "bi"]) }}

WITH 
  ----------------------------------------------------
  -->>                 VALOR DOLAR                <<--
  ----------------------------------------------------   
  dolar as (
    SELECT DISTINCT
      Fecha, Valor as valor_dolar_cierre
    FROM {{source('ingestas_api','cmf_api_dolar')}}),
  dates as (
    SELECT 
      dia as fecha
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)) dia
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
       valor_dolar_cierre as valor_dolar_cierre_real_dia,
       IF(valor_dolar_cierre is null, last_value(valor_dolar_cierre ignore nulls) OVER (ORDER BY fecha rows between unbounded preceding and 1 preceding), valor_dolar_cierre) valor_dolar_cierre_arrastrado,

    FROM cruce
)

SELECT *
FROM dolar_final