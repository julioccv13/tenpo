DROP TABLE IF EXISTS `tenpo-bi.tmp.ahorro_bencina{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.ahorro_bencina{{ds_nodash}}` AS 
(

SELECT a.user AS identity, 
       CONCAT('$', REPLACE(FORMAT("%'d", CAST(SUM(monto) AS INT64)), ',', '.')) AS ahorro_bencina
FROM `tenpo-bi-prod.economics.economics` a
WHERE linea IN ('reward') AND monto IS NOT NULL AND monto != 0
  AND fecha >= DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 MONTH, MONTH)
  AND fecha < DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH), MONTH)
  AND (comercio LIKE '%BENCINA%' OR comercio LIKE '%PETRO%' OR comercio LIKE '%APP BENCINA%' OR comercio LIKE '%copec%' OR comercio LIKE '%muevo%' OR comercio LIKE '%shell%'
    OR comercio LIKE '%petrobras%' OR comercio LIKE '%copiloto%' OR comercio LIKE '%petrob%' OR comercio LIKE '%terpel%')
GROUP BY a.user
ORDER BY SUM(monto) DESC

)
