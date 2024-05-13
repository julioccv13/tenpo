{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH target AS 

    (SELECT 
        CONCAT(FORMAT_DATE("%Y%m", creacion),"01") as month
        ,creacion
        ,id
        ,user
        ,estado
        ,grupo
        ,tipo
        ,producto
        ,subtipificacion
        ,interacciones_agente
        ,IFNULL(ultimo_segmento_rfmp_60,'ninguno') rfmp_60
        ,COUNT(user) OVER (PARTITION BY user,  CONCAT(FORMAT_DATE("%Y%m", creacion),"01") ORDER BY CONCAT(FORMAT_DATE("%Y%m", creacion),"01") DESC) as reincidencia_mensual
        ,COUNT(user) OVER (PARTITION BY user) reincidencia_historica
--FROM `tenpo-bi-prod.external.tickets_freshdesk`
FROM {{ ref('tickets_freshdesk') }}

WHERE user IS NOT NULL
AND length(user) > 1
ORDER BY user DESC)

SELECT * 
FROM target
WHERE reincidencia_mensual > 1
ORDER BY month  DESC