DROP TABLE IF EXISTS `tenpo-bi.tmp.cashback_bencina_text{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.cashback_bencina_text{{ds_nodash}}` AS 
(

with q as (

SELECT user, SUM(monto) AS monto_total
FROM `tenpo-bi-prod.economics.economics`
WHERE linea IN ('reward') 
  AND fecha >= DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 1 MONTH, MONTH)
  AND fecha < DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH), MONTH)
  AND (comercio LIKE '%BENCINA%' OR comercio LIKE '%PETRO%' OR comercio LIKE '%APP BENCINA%' OR comercio LIKE '%copec%' OR comercio LIKE '%muevo%' OR comercio LIKE '%shell%'
    OR comercio LIKE '%petrobras%' OR comercio LIKE '%copiloto%' OR comercio LIKE '%petrob%' OR comercio LIKE '%terpel%')
  AND monto IS NOT NULL AND monto != 0
GROUP BY user

)
select user as identity, CASE WHEN monto_total >= 1000 THEN "cashback_bencina_mayor_mil" ELSE "cashback_bencina_menor_mil" END AS ahorro_cashback_text
from q 
where monto_total is not null and monto_total !=0

)
