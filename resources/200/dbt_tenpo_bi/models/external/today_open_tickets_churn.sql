
/* CATEGORIA ADICIONAL - CHURN Y ADEMAS TIENEN UN TICKET ABIERTO  */

{{ 
  config(
    materialized='table',
  ) 
}}

SELECT 
        DISTINCT
        email, 
       'open ticket & churn' as reason,
       true as no_molestar
FROM {{source('freshdesk','freshdesk')}} 
JOIN {{ source('tenpo_users', 'users') }} u on u.id = user_id   
WHERE email IN (SELECT  DISTINCT email FROM {{ ref('users_tenpo')  }} WHERE churn = 1 )
