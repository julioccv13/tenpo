{{ 
  config(
    tags=["hourly", "bi"], 
    materialized='table',
    partition_by = { 'field': 'fecha', 'data_type': 'date' }
  )
}}

SELECT 
    date(ob_completed_at,'America/Santiago') fecha,
    count(distinct id) onboardings
FROM {{ ref('users_tenpo') }}
WHERE status_onboarding = 'completo'
GROUP BY 1