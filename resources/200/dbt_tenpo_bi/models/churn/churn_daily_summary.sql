 {{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}


 SELECT distinct
  Fecha_Fin_Analisis_DT
  ,count(distinct user) usuarios
  ,count(distinct case when state != "cuenta_cerrada" then user else null end) as cliente
  ,count(distinct case when state = "activo" then user else null end) clientes_ready 
  ,count(distinct case when state = "cuenta_cerrada" then user else null end) as cuentas_cerradas
  ,count(distinct case when state in ("churneado", "cuenta_cerrada") and last_state in ("onboarding", "activo") then user else null end) churn_periodo  
  ,count(distinct case when last_state = "onboarding"  then user else null end) ob_periodo 
  ,count(distinct case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then user else null end) cierres_cuenta_periodo 
  ,count(distinct case when state in ("churneado", "cuenta_cerrada")  and last_state in ("onboarding", "activo") and cierre_involuntario is false then user else null end) churn_periodo_voluntario 
  ,count(distinct case when state in ("churneado", "cuenta_cerrada")  and last_state in ("onboarding", "activo") and cierre_involuntario is true then user else null end) churn_periodo_involuntario
  ,count(distinct case when state = "activo" and last_state in ("churneado") then user else null end) retorno_periodo  
  ,count(distinct case when state = "activo" and last_state in ("churneado") and cierre_involuntario is false then user else null end) retorno_periodo_voluntario
  ,count(distinct case when state = "activo" and last_state in ("churneado") and cierre_involuntario is true then user else null end) retorno_periodo_involuntario
FROM {{ ref('daily_churn') }}
GROUP BY  
  Fecha_Fin_Analisis_DT