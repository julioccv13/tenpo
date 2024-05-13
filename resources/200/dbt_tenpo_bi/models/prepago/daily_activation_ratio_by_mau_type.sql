{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}


WITH 
  dates AS (
    SELECT 
      dia
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2020-01-01'), CURRENT_DATE("America/Santiago"), INTERVAL 1 DAY)) dia
    ORDER BY 
      dia DESC
  ), churn_mensual as (
    SELECT
      Fecha_Fin_Analisis_DT
      ,user
      ,cliente
      ,cierre_cuenta 
      ,churn
    FROM {{ref('daily_churn')}}  --`tenpo-bi-prod.churn.daily_churn`  
  ), usuarios_tarjetas as (
    SELECT DISTINCT
      a.Fecha_Fin_Analisis_DT  
      ,CAST(IF(fecha_activacion <'2020-01-01', '2020-01-01', fecha_activacion) AS DATE) fecha_activacion
      ,a.user 
      ,tipo
      ,cliente
    FROM {{source('tarjeta','activacion_tarjeta_daily')}}  a --`tenpo-bi.tarjeta.activacion_tarjeta_daily`  usuarios_tarjetas
      LEFT JOIN churn_mensual b 
      ON a.user = b.user  
    WHERE TRUE
      AND tipo is not null
--       AND usuarios_tarjetas.user  = "3225bdb1-a27e-4971-b1a3-8e18e8852515"
  ), tarjetas as (
    SELECT
      a.Fecha_Fin_Analisis_DT fecha
      ,mau_type
      ,COUNT(DISTINCT a.user ) tarjetas_activadas
      ,COUNT(DISTINCT case when cliente is true then a.user else null end) cuenta_clientes_tarjeta
      ,COUNT(DISTINCT case when cliente is true and tipo = "PHYSICAL" then a.user else null end) cuenta_physical
      ,COUNT(DISTINCT case when cliente is true and tipo = "VIRTUAL" then a.user else null end) cuenta_virtual
    FROM usuarios_tarjetas a
      LEFT JOIN {{ref('mau_type_per_month')}} c 
      ON a.user = c.user and c.mes = date_trunc(a.Fecha_Fin_Analisis_DT, month)
    GROUP BY 
      1 ,2
  ), compras_exitosas as (
    SELECT
       fecha
       ,mau_type
      ,COUNT(DISTINCT a.user ) cuenta_compras_exitosas
    FROM {{ref('economics')}} a --`tenpo-bi-prod.economics.economics` 
      LEFT JOIN {{ref('mau_type_per_month')}} c 
      ON a.user = c.user and c.mes = date_trunc(a.fecha, month)
    WHERE 
      linea in ( 'mastercard', 'mastercard_physical')
    GROUP BY 
      1, 2
  )
   
   SELECT
    *
   FROM tarjetas 
   LEFT JOIN compras_exitosas USING(fecha, mau_type)