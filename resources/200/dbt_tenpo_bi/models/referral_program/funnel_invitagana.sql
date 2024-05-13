{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT 
  f.*,
  camada_ob_referrer,
  segment_referrer,
  score_referrer
FROM  {{ ref('funnel_tenpo') }}  f
LEFT JOIN {{ ref('parejas_iyg') }} ON user = uuid
WHERE 
  source like '%IG%' 
  AND f.grupo  is not null
