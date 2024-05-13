
{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "is_tef_validated",
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}

SELECT DISTINCT
  Fecha_Fin_Analisis_DT fecha,
  CASE WHEN is_tef_validated = true THEN 'Validación TEF' WHEN is_tef_validated = false THEN 'Validación Selfie' ELSE null END AS  is_tef_validated,
  COUNT( DISTINCT user) as total_ab,
FROM  {{source('productos_tenpo','tenencia_productos_tenpo')}}
JOIN {{ ref('economics') }}  USING(user)
JOIN {{ ref('users_tenpo') }}   ON user = id
WHERE 
  fecha >=  date_sub(Fecha_Fin_Analisis_DT,interval 29 day)  
  AND fecha <= Fecha_Fin_Analisis_DT
  AND is_tef_validated is not null
  AND linea not in ('reward')
  AND state in (4,7,8,21,22)
GROUP BY 1,2
