/*Cruza quienes son MAUS del mes con respecto a los tickets contra el mismo mes*/
{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  )
}}

WITH tickets AS ( 
    SELECT 
         *
        ,CONCAT(format_date("%Y%m", creacion),"01") month
        ,CONCAT(user,CONCAT(format_date("%Y%m", creacion),"01")) llave
    FROM {{ ref('tickets_freshdesk') }}
)
SELECT 
    tickets.*
    ,IFNULL(MAUS.es_mau_del_mes,false) es_mau_del_mes
FROM tickets
LEFT JOIN (

    WITH maus_montly AS (

        SELECT
            CONCAT(format_date("%Y%m", fecha),"01") month
            ,trx_timestamp
            ,user
            ,CONCAT(user,CONCAT(format_date("%Y%m", fecha),"01")) llave
            ,email
            ,trx_id
            ,nombre
            ,comercio
            ,ROW_NUMBER() OVER (PARTITION BY user , CONCAT(format_date("%Y%m", fecha),"01") ORDER BY trx_timestamp ASC) row
            ,true as es_mau_del_mes
FROM {{ ref('economics') }}
WHERE nombre <> 'Premio Tenpo'
ORDER BY email DESC, trx_timestamp ASC 

)

SELECT * EXCEPT (row) 
FROM maus_montly
WHERE row = 1
ORDER BY trx_timestamp DESC
) MAUS using (llave)

ORDER BY creacion DESC