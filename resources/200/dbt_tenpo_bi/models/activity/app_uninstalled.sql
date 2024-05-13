{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
  ) 
}}


SELECT 
   fecha_hora,
   identity as user,
    ct_app_v,
    ct_os_v,
    plataform
FROM {{source('clevertap','events')}} 
WHERE event = 'App Uninstalled'