{{ 
    config(
          materialized='view',  
          tags=["business_dashboard", "daily", "bi"]
          )
}} 

SELECT DISTINCT 
    *
FROM  {{source('presupuesto','presupuesto_consolidado')}}
WHERE true
QUALIFY row_number() over (partition by categoria,linea, tipo_usuario, canal, moneda, fecha order by fecha_actualizacion desc) = 1
