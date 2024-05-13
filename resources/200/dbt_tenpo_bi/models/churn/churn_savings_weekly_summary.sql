{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

 WITH 
  data as (
    SELECT
      DATE(DATE_TRUNC(periodo_legible, week)) periodo
      ,DATE_TRUNC(Fecha_Fin_Analisis_DT, week) Fecha_Inicio_Analisis_DT
      ,Fecha_Fin_Analisis_DT
      ,'global' dimension
      ,count(distinct user) usuarios
      ,count(distinct case when state = "activo" then user else null end) clientes   
      ,count(distinct case when state in ("churneado", "cuenta_cerrada") and last_state in ("onboarding", "activo") then user else null end) churn_periodo  
      ,count(distinct case when last_state = "onboarding"  then user else null end) ob_periodo 
      ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then user else null end) cierres_cuenta_periodo 
      ,count(distinct case when state = "activo" and last_state in ("churneado") then user else null end) retorno_periodo  
    FROM {{source('churn','tablon_weekly_eventos_churn_bolsillo')}}
    GROUP BY  
      1,2,3
    ),
   resultado_global as (
      SELECT
        *
        ,LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo) clientes_periodo_anterior
        ,SAFE_DIVIDE((churn_periodo - retorno_periodo ), LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo)) churn
      FROM data
      WHERE 
        periodo = Fecha_Inicio_Analisis_DT
        )
        
   
SELECT
  *
FROM resultado_global 
WHERE 
  churn is not null

