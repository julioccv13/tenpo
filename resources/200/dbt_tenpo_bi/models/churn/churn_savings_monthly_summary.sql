{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  data as (
    SELECT
      DATE(periodo_legible) periodo
      ,Fecha_Fin_Analisis_DT
      ,'global' dimension
      ,count(distinct user) usuarios
      ,count(distinct case when state = "activo" then user else null end) clientes   
      ,count(distinct case when state in ("churneado") and last_state in ("onboarding", "activo") then user else null end) churn_periodo  
      ,count(distinct case when last_state = "onboarding"  then user else null end) ob_periodo 
      ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then user else null end) cierres_cuenta_periodo 
      ,count(distinct case when state = "activo" and last_state in ("churneado") then user else null end) retorno_periodo  
    FROM {{source('churn','tablon_monthly_eventos_churn_bolsillo')}}  
    GROUP BY  
      1,2,3
    ),
   resultado_global as (
      SELECT
        *
        ,LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo) clientes_periodo_anterior
        ,(churn_periodo - retorno_periodo )/ LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo) churn
      FROM data
      WHERE 
        periodo = DATE_TRUNC(Fecha_Fin_Analisis_DT, MONTH)
        )
    SELECT * FROM resultado_global
        