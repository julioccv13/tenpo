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
      ),
  churn_mensual as (
    SELECT
      Fecha_Fin_Analisis_DT
      ,user
      ,cliente
      ,cierre_cuenta 
      ,churn
    FROM {{ref('daily_churn')}}  --`tenpo-bi-prod.churn.daily_churn`  
    ORDER BY 1 DESC
       ),
   usuarios_tarjetas as (
    SELECT DISTINCT
      usuarios_tarjetas.Fecha_Fin_Analisis_DT  
      ,CAST(IF(fecha_activacion <'2020-01-01', '2020-01-01', fecha_activacion) AS DATE) fecha_activacion
      ,usuarios_tarjetas.user 
      ,tipo
      ,cliente
    FROM {{source('tarjeta','activacion_tarjeta_daily')}}  usuarios_tarjetas --`tenpo-bi.tarjeta.activacion_tarjeta_daily`  usuarios_tarjetas
    LEFT JOIN churn_mensual ON usuarios_tarjetas.user = churn_mensual.user  
    WHERE TRUE
      AND tipo is not null
--       AND usuarios_tarjetas.user  = "3225bdb1-a27e-4971-b1a3-8e18e8852515"
    ORDER BY 1 DESC
    ),
  tarjetas as (
    SELECT
      usuarios_tarjetas.Fecha_Fin_Analisis_DT fecha
      ,COUNT(DISTINCT usuarios_tarjetas.user ) tarjetas_activadas
      ,COUNT(DISTINCT case when cliente is true then usuarios_tarjetas.user else null end) cuenta_clientes_tarjeta
      ,COUNT(DISTINCT case when cliente is true and tipo = "PHYSICAL" then usuarios_tarjetas.user else null end) cuenta_physical
      ,COUNT(DISTINCT case when cliente is true and tipo = "VIRTUAL" then usuarios_tarjetas.user else null end) cuenta_virtual
    FROM usuarios_tarjetas
    GROUP BY 
      1 
    ORDER BY 
      1 DESC
      ),
  compras_exitosas as (
    SELECT
       fecha
      ,COUNT(DISTINCT user ) cuenta_compras_exitosas
    FROM {{ref('economics')}}  --`tenpo-bi-prod.economics.economics` 
    WHERE 
      linea in ( 'mastercard', 'mastercard_physical')
    GROUP BY 
      1
    ORDER BY 
      1 DESC
      )
   
   SELECT
    *
   FROM tarjetas 
   LEFT JOIN compras_exitosas USING(fecha)
   ORDER BY 
      1 DESC