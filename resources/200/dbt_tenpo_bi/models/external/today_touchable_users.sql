{{ 
  config(
    materialized='table',
  ) 
}}

SELECT 
    DISTINCT * , 'none' as reason 
FROM {{ ref('users_last_touch') }} --`tenpo-bi-dev.external.users_last_touch` 
WHERE no_molestar = false
/* EXCLUIR A LOS QUE YA SON CHURN Y TIENEN TICKETS ABIERTOS PARA NO DUPLICAR USUARIOS  */
AND email NOT IN (
                    SELECT 
                          DISTINCT 
                          email 
                    FROM {{ ref('today_open_tickets') }} )
                    /*
                    UNION ALL 
                    (SELECT 
                          DISTINCT 
                          email 
                    FROM {{ ref('today_churn_users') }} )
                    */
