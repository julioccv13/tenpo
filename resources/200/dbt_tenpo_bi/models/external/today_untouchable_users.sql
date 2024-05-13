{{ 
  config(
    materialized='table',
  ) 
}}

SELECT 
    DISTINCT *, 'exceed touches' as reason 
FROM {{ ref('users_last_touch') }} 
WHERE no_molestar = true
/* EXCLUIR A LOS QUE YA SON CHURN Y TIENEN TICKETS ABIERTOS PARA NO DUPLICAR USUARIOS  */
AND email NOT IN (
                    SELECT 
                          DISTINCT 
                          email 
                    FROM {{ ref('today_open_tickets') }})

                    /*
                    UNION ALL 
                    (SELECT 
                          DISTINCT 
                          email 
                    FROM {{ ref('today_churn_users') }}))*/