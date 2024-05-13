{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
  ) 
}}

    SELECT
      *
    FROM {{ ref('daily_churn') }}
    WHERE 
      Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) from {{ ref('daily_churn') }})