{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}

SELECT 
    DATE(ob_completed_at,'America/Santiago') fecha,
    id as user,
    state
FROM {{ ref('users_tenpo') }}
WHERE state in (4,7,8,21,22)
AND ob_completed_at IS NOT NULL

