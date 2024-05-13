{{ 
    config(
          materialized='view',  
          tags=["business_dashboard", "daily", "bi"]
          )
}} 


SELECT
    DATE_TRUNC(fecha, ISOWEEK) semana,  
    categoria,
    IF(moneda is null, 'clp', moneda) moneda,
    SUM(presupuesto) presupuesto,
    linea,
    tipo_usuario,
    canal
FROM  {{ref('ultimo_presupuesto')}}
GROUP BY 
    categoria,linea, tipo_usuario, canal, moneda, semana