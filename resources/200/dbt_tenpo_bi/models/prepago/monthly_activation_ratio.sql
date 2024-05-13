{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  dates AS (
    SELECT 
      mes
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2020-01-01'), CURRENT_DATE("America/Santiago"), INTERVAL 1 MONTH)) mes
    ORDER BY 
      mes DESC
      ),
  churn_mensual as (
    SELECT
      DATE(periodo_legible) mes
      ,Fecha_Fin_Analisis_DT
      ,user
      ,case when state = "activo" then 1 else 0 end cliente
      ,case when state in ("churneado", "cuenta_cerrada") and last_state in ("onboarding", "activo") then 1 else 0 end churn_periodo  
      ,case when last_state = "onboarding"  then 1 else 0 end ob_periodo 
      ,case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then 1 else 0 end cierres_cuenta_periodo 
      ,case when state = "activo" and last_state in ("churneado") then 1 else 0 end retorno_periodo  
    FROM {{source('churn','tablon_monthly_eventos_churn')}}  
    WHERE TRUE
       AND DATE(periodo_legible) = DATE_TRUNC(Fecha_Fin_Analisis_DT, MONTH)
--        AND  user = "92cc7595-dc0d-44ad-8481-b2c8028cbddb"
    ORDER BY 1 DESC
       ),
   usuarios_tarjetas as (
    SELECT DISTINCT
      DATE_TRUNC(usuarios_tarjetas.Fecha_Fin_Analisis_DT , month) mes
      ,usuarios_tarjetas.Fecha_Fin_Analisis_DT  
      ,CAST(IF(fecha_activacion <'2020-01-01', '2020-01-01', fecha_activacion) AS DATE) fecha_activacion
      ,usuarios_tarjetas.user 
      ,tipo
      ,cliente
    FROM {{source('tarjeta','activacion_tarjeta_monthly')}}  usuarios_tarjetas
    LEFT JOIN churn_mensual ON usuarios_tarjetas.user = churn_mensual.user AND DATE_TRUNC(usuarios_tarjetas.Fecha_Fin_Analisis_DT , month) = churn_mensual.mes
    WHERE TRUE
      AND tipo is not null
--       AND usuarios_tarjetas.user  = "3225bdb1-a27e-4971-b1a3-8e18e8852515"
    ORDER BY 1 DESC
    ),
  tarjetas as (
    SELECT
      usuarios_tarjetas.mes mes
      ,COUNT(DISTINCT usuarios_tarjetas.user ) tarjetas_activadas
      ,COUNT(DISTINCT case when cliente = 1 then usuarios_tarjetas.user else null end) cuenta_clientes_tarjeta
      ,COUNT(DISTINCT case when cliente = 1 and tipo = "PHYSICAL" then usuarios_tarjetas.user else null end) cuenta_physical
      ,COUNT(DISTINCT case when cliente = 1 and tipo = "VIRTUAL" then usuarios_tarjetas.user else null end) cuenta_virtual
    FROM usuarios_tarjetas
    GROUP BY 
      1 
    ORDER BY 
      1 DESC
      ),
  compras_exitosas as (
    SELECT
      DATE_TRUNC(fecha , month) mes
      ,COUNT(DISTINCT user ) cuenta_compras_exitosas
    FROM {{ref('economics')}} 
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
   LEFT JOIN compras_exitosas USING(mes)
   ORDER BY 
      1 DESC
      