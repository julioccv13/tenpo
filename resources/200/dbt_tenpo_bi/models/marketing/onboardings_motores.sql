{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}

SELECT
    IFNULL(B.motor,'desconocido') motor,
    DATE(A.ob_completed_at,'America/Santiago') fecha,
    A.id as user,
    A.state
FROM {{ ref('users_tenpo') }} A
LEFT JOIN  {{ ref('appsflyer_users') }} B on A.id = B.customer_user_id
WHERE state in (4,7,8,21,22)
AND A.ob_completed_at IS NOT NULL