{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH promotores as 
(SELECT 
        DATE(CONCAT(CONCAT(FORMAT_DATE("%Y-%m", submitted_at),"-"),"01")) submitted_at,
        clasificacion,
        count(*) as count
--FROM `tenpo-bi-prod.external.nps`
FROM {{ ref('nps') }}
WHERE clasificacion = 'promotor'
GROUP BY 1,2
ORDER BY 1 DESC),
neutros AS
(SELECT 
        DATE(CONCAT(CONCAT(FORMAT_DATE("%Y-%m", submitted_at),"-"),"01")) submitted_at,
        clasificacion,
        COUNT(*) as count
--FROM `tenpo-bi-prod.external.nps`
FROM {{ ref('nps') }}
WHERE clasificacion = 'neutro'
GROUP BY 1,2
ORDER BY 1 DESC),

detractor AS 
(SELECT 
        DATE(CONCAT(CONCAT(FORMAT_DATE("%Y-%m", submitted_at),"-"),"01")) submitted_at,
        clasificacion,
        count(*) as count
--FROM `tenpo-bi-prod.external.nps`
FROM {{ ref('nps') }}
WHERE clasificacion = 'detractor'
GROUP BY 1,2
ORDER BY 1 DESC)

SELECT  
        promotores.submitted_at
        ,IFNULL(promotores.count,0) promotores
        ,IFNULL(neutros.count,0) neutro
        ,IFNULL(detractor.count,0) detracotres
FROM promotores
LEFT JOIN neutros USING (submitted_at)
LEFT JOIN detractor USING (submitted_at)
ORDER BY 1 DESC