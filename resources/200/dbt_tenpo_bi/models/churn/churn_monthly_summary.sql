{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  data_flag as (
    SELECT
      DATE(periodo_legible) periodo
      ,Fecha_Fin_Analisis_DT
      ,CASE WHEN coalesced_f_actividad_app is true THEN 'con actividad' WHEN coalesced_f_actividad_app is false THEN 'sin actividad' ELSE null END as dimension
      ,count(distinct user) usuarios
      ,count(distinct case when state = "activo" then user else null end) clientes_ready
      ,count(distinct case when state != "cuenta_cerrada" then user else null end) clientes
      ,count(distinct case when state != "cuenta_cerrada" and  cierre_involuntario is false  then user else null end) clientes_voluntario
      ,count(distinct case when state = "activo" and cierre_involuntario is false then user else null end) clientes_ready_voluntarios   
      ,count(distinct case when state = "activo" and cierre_involuntario is true then user else null end) clientes_ready_involuntarios   
      ,count(distinct case when state in ("churneado", "cuenta_cerrada") and last_state in ("onboarding", "activo") then user else null end) churn_periodo  
      ,count(distinct case when last_state = "onboarding"  then user else null end) ob_periodo 
      ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then user else null end) cierres_cuenta_periodo 
      ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") and cierre_involuntario is false then user else null end) cierres_cuenta_periodo_voluntario
      ,count(distinct case when state in ("churneado", "cuenta_cerrada")  and last_state in ("onboarding", "activo") and cierre_involuntario is false then user else null end) churn_periodo_voluntario 
      ,count(distinct case when state in ("churneado", "cuenta_cerrada")  and last_state in ("onboarding", "activo") and cierre_involuntario is true then user else null end) churn_periodo_involuntario
      ,count(distinct case when state = "activo" and last_state in ("churneado") then user else null end) retorno_periodo  
      ,count(distinct case when state = "activo" and last_state in ("churneado") and cierre_involuntario is false then user else null end) retorno_periodo_voluntario
      ,count(distinct case when state = "activo" and last_state in ("churneado") and cierre_involuntario is true then user else null end) retorno_periodo_involuntario
    FROM {{source('churn','tablon_monthly_eventos_churn')}}  
    GROUP BY  
      1,2,3
    ),
   resultado_flag_actividad as (
      SELECT
        *
        ,LAG(clientes_ready) OVER (PARTITION BY dimension ORDER BY periodo) clientes_periodo_anterior
        ,LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo) clientes_periodo_anterior
        ,SAFE_DIVIDE((churn_periodo - retorno_periodo ), LAG(clientes_ready) OVER (PARTITION BY dimension ORDER BY periodo)) churn_ready
        ,SAFE_DIVIDE((churn_periodo_involuntario - retorno_periodo_involuntario ), LAG(clientes_ready_involuntarios) OVER (PARTITION BY dimension ORDER BY periodo)) churn_ready_involuntario
        ,SAFE_DIVIDE((churn_periodo_voluntario - retorno_periodo_voluntario ), LAG(clientes_ready_voluntarios) OVER (PARTITION BY dimension ORDER BY periodo)) churn_ready_voluntario
        
        ,SAFE_DIVIDE((cierres_cuenta_periodo  ), LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo)) churn_clientes
        ,SAFE_DIVIDE((cierres_cuenta_periodo_voluntario  ), LAG(clientes_voluntario) OVER (PARTITION BY dimension ORDER BY periodo)) churn_clientes_voluntario



      FROM data_flag
      WHERE 
        periodo = DATE_TRUNC(Fecha_Fin_Analisis_DT, MONTH)
    ),
    data as (
    SELECT
      DATE(periodo_legible) periodo
      ,Fecha_Fin_Analisis_DT
      ,'global' dimension
      ,count(distinct user) usuarios
      ,count(distinct case when state = "activo" then user else null end) clientes_ready
      ,count(distinct case when state != "cuenta_cerrada" then user else null end) clientes
      ,count(distinct case when state != "cuenta_cerrada" and cierre_involuntario is false  then user else null end) clientes_voluntario
      ,count(distinct case when state = "activo" and cierre_involuntario is false then user else null end) clientes_ready_voluntarios   
      ,count(distinct case when state = "activo" and cierre_involuntario is true then user else null end) clientes_ready_involuntarios   
      ,count(distinct case when state in ("churneado", "cuenta_cerrada") and last_state in ("onboarding", "activo") then user else null end) churn_periodo  
      ,count(distinct case when last_state = "onboarding"  then user else null end) ob_periodo 
      ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then user else null end) cierres_cuenta_periodo 
      ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") and cierre_involuntario is false then user else null end) cierres_cuenta_periodo_voluntario
      ,count(distinct case when state in ("churneado", "cuenta_cerrada")  and last_state in ("onboarding", "activo") and cierre_involuntario is false then user else null end) churn_periodo_voluntario 
      ,count(distinct case when state in ("churneado", "cuenta_cerrada")  and last_state in ("onboarding", "activo") and cierre_involuntario is true then user else null end) churn_periodo_involuntario
      ,count(distinct case when state = "activo" and last_state in ("churneado") then user else null end) retorno_periodo  
      ,count(distinct case when state = "activo" and last_state in ("churneado") and cierre_involuntario is false then user else null end) retorno_periodo_voluntario
      ,count(distinct case when state = "activo" and last_state in ("churneado") and cierre_involuntario is true then user else null end) retorno_periodo_involuntario
    FROM {{source('churn','tablon_monthly_eventos_churn')}}  
    GROUP BY  
      1,2,3
    ),
   resultado_global as (
      SELECT
        *
        ,LAG(clientes_ready) OVER (PARTITION BY dimension ORDER BY periodo) clientes_ready_periodo_anterior
        ,LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo) clientes_periodo_anterior
        ,SAFE_DIVIDE((churn_periodo - retorno_periodo ), LAG(clientes_ready) OVER (PARTITION BY dimension ORDER BY periodo)) churn_ready
        ,SAFE_DIVIDE((churn_periodo_involuntario - retorno_periodo_involuntario ), LAG(clientes_ready_involuntarios) OVER (PARTITION BY dimension ORDER BY periodo)) churn_ready_involuntario
        ,SAFE_DIVIDE((churn_periodo_voluntario - retorno_periodo_voluntario ), LAG(clientes_ready_voluntarios) OVER (PARTITION BY dimension ORDER BY periodo)) churn_ready_voluntario

        ,SAFE_DIVIDE((cierres_cuenta_periodo  ), LAG(clientes) OVER (PARTITION BY dimension ORDER BY periodo)) churn_clientes
        ,SAFE_DIVIDE((cierres_cuenta_periodo_voluntario  ), LAG(clientes_voluntario) OVER (PARTITION BY dimension ORDER BY periodo)) churn_clientes_voluntario
      FROM data
      WHERE 
        periodo = DATE_TRUNC(Fecha_Fin_Analisis_DT, MONTH)
        )
        
    SELECT
    *
    FROM(
      SELECT
        *
      FROM resultado_global 
      WHERE churn_ready is not null
      
      UNION ALL
      
      SELECT
        *
      FROM resultado_flag_actividad   
      WHERE churn_ready is not null
      ) 