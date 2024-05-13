{{ 
  config(
    tags=["daily", "bi","sessions"],
    materialized='table', 
    cluster_by = "tenpo_uuid"
  ) 
}}

SELECT DISTINCT 
  PARSE_DATE("%Y%m%d", CAST(date as STRING)) fecha,
  DATE_TRUNC(PARSE_DATE("%Y%m%d", CAST(date as STRING)), month) mes_visita, 
  b.id tenpo_uuid,
  a.session_id,
  a.session_lenght
FROM {{source('clevertap','events')}} a
JOIN {{ ref('users_tenpo') }} b  on b.id = a.identity
WHERE
  event = 'Session Concluded'
  AND state in (4,7,8,21,22)
  AND session_lenght > 0
  AND session_lenght <= 1200
QUALIFY
 ROW_NUMBER() OVER (PARTITION BY  a.email , session_id , date ORDER BY fecha_hora ) = 1