{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}



WITH data AS 

(SELECT 
      fecha_ob,
      'nuevo ob' as origen,
      count(distinct uuid) obs 
FROM {{ ref('funnel_tenpo_ob_ligth') }} 
where paso = '10. OB exitoso'
GROUP BY 1,2

UNION ALL 

SELECT 
    fecha_ob,
    'antiguo ob' as origen,
    count(distinct uuid) obs
FROM {{ ref('funnel_tenpo') }} 
where paso = '7. OB exitoso'
GROUP BY 1,2 

)

SELECT 
      * 
FROM data
ORDER BY 1 DESC