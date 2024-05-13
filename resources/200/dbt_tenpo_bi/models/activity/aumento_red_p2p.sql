{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "user"
  ) 
}}

WITH target AS (SELECT distinct
        fecha_hora
        ,CONCAT(FORMAT_DATE("%Y%m", fecha_hora),"01") format_date
        ,{{ hash_sensible_data('email') }} email
        ,identity user
        ,tenpoContacts
        ,totalContacts
        ,MIN(fecha_hora) OVER (PARTITION BY email, CONCAT(FORMAT_DATE("%Y%m", fecha_hora),"01") ) inicio_de_mes
        ,MIN(tenpoContacts) OVER (PARTITION BY email, CONCAT(FORMAT_DATE("%Y%m", fecha_hora),"01")) red_inicio_mes
        ,MAX(fecha_hora) OVER (PARTITION BY email, CONCAT(FORMAT_DATE("%Y%m", fecha_hora),"01")) fin_de_mes
        ,MAX(tenpoContacts) OVER (PARTITION BY email, CONCAT(FORMAT_DATE("%Y%m", fecha_hora),"01")) red_fin_mes
--FROM {{source('clevertap','events')}}
from {{ source('clevertap', 'events') }}
WHERE event = 'InformaciÃ³n de contactos Tenpo'
order by user desc, fecha_hora desc) 

SELECT 
        distinct 
        *
        ,(red_fin_mes - red_inicio_mes) as variacion_de_red 
FROM target
WHERE fecha_hora = fin_de_mes
AND CONCAT(FORMAT_DATE("%Y%m", fecha_hora),"01") = CONCAT(FORMAT_DATE("%Y%m", current_date()),"01")
--ORDER BY 1 DESC