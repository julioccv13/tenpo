{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
  ) 
}}


SELECT 
   dt as fecha_hora,
   profile.identity as user,
   deviceinfo.appversion as ct_app_v,
   deviceinfo.osversion ct_os_v,
   profile.platform platform,    
FROM {{source('clevertap_raw','clevertap_gold_external')}} 
WHERE eventname = 'Hace login'

-- SELECT 
--    fecha_hora,
--    identity as user,
--     ct_app_v,
--     ct_os_v,
--     plataform
-- FROM {{source('clevertap','events')}} 
-- WHERE event = 'Hace login'