{{ 
  config(
    materialized='table',
  ) 
}}

SELECT 
     DISTINCT 
     email,
     'churn' as reason,
     true as no_molestar
FROM  {{ ref('users_tenpo') }} 
WHERE churn = 1
/* EXCLUIR A LOS QUE SE INTERSECTAN (TIENE TICKET ABIERTO Y SON CHURN) */
AND email NOT IN (SELECT 
                        DISTINCT 
                        email 
                  FROM {{ ref('today_open_tickets_churn') }})  