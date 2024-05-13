{{ 
  config(
    tags=["hourly", "bi"],
    materialized='ephemeral',
    enabled=True
  ) 
}}

SELECT
    max(fecha) as ultima_fecha_envio_nps
FROM {{ref('clevertap_raw')}}
where eventname like '%Notification Sent%'
and eventprops.campaign_name like '%NPS Recurrente%'