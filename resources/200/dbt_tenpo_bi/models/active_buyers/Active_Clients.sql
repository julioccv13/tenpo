{{ 
  config(
    tags=["daily", "bi"],
    materialized='table',
    project=env_var('DBT_PROJECT2', 'tenpo-datalake-sandbox'),
    schema='Camadas_v2'
  ) 
}}



SELECT DISTINCT user,DATE_TRUNC(Fecha_Fin_Analisis_DT, MONTH) AS month
FROM  {{ ref('daily_churn') }}
WHERE state = 'activo'