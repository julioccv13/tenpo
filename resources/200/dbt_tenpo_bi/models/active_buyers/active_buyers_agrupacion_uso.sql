{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "agrupacion_uso",
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}

SELECT DISTINCT
  Fecha_Fin_Analisis_DT fecha,
  agrupacion agrupacion_uso,
  COUNT( DISTINCT user) as total_ab,
FROM {{source('productos_tenpo','tenencia_productos_tenpo')}}
JOIN {{ ref('economics') }}  USING(user)
WHERE 
  fecha >=  date_sub(Fecha_Fin_Analisis_DT,interval 29 day)  
  AND fecha <= Fecha_Fin_Analisis_DT
  AND agrupacion is not null
  AND lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
GROUP BY 1,2
-- prueba commit