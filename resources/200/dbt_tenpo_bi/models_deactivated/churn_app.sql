{{ config(tags=["daily", "bi"], materialized='table') }}

SELECT 
  *
FROM {{source('churn','churn_tenpo')}}
WHERE 
  Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM {{source('churn','churn_tenpo')}})
