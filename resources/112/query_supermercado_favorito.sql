-- Creación de property para seleccionar el supermercado favorito de un usuario, consierando los supermercados principales de Chile y los que más se repiten.
DROP TABLE IF EXISTS `tenpo-bi.tmp.supermercado_favorito_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.supermercado_favorito_{{ds_nodash}}` AS 

(

WITH usuario_supermercado AS (
SELECT A.identity, 
CASE 
  WHEN Supermercado LIKE '%JUMBO%' THEN 'Jumbo'
  WHEN Supermercado LIKE '%Unimarc%' THEN 'Unimarc'
  WHEN Supermercado LIKE '%MAXIK%' THEN 'MAXIK'
  WHEN Supermercado LIKE '%STA ISABEL%' THEN 'Santa Isabel'
  WHEN Supermercado LIKE '%OK MARKET%' THEN 'OK Market'
  WHEN Supermercado LIKE '%EL TREBOL%' THEN 'El Trebol'
  WHEN Supermercado LIKE '%TOTTUS%' THEN 'Tottus'
  WHEN Supermercado LIKE '%SUPER GANGA%' THEN 'Super Ganga'
  ELSE 'Otro'
  END AS supermercado_favorito
FROM (
  SELECT A.user AS identity, comercio AS Supermercado, N AS n_compras, ROW_NUMBER() OVER (PARTITION BY A.user ORDER BY A.N DESC) AS rn_up
  FROM (SELECT user, comercio, COUNT(1) AS N
    FROM `tenpo-bi-prod.economics.economics`
    WHERE linea IN ('mastercard','mastercard_physical')
      AND rubro_recod = 'Supermercados'
    GROUP BY user, comercio
    ) AS A 
  ) AS A
WHERE A.rn_up = 1
)
SELECT A.id AS identity, 
IFNULL(CAST(B.supermercado_favorito AS string),'N/A') AS supermercado_favorito
FROM `tenpo-bi-prod.users.users_tenpo`  AS A
LEFT JOIN usuario_supermercado AS B ON A.id = B.identity
WHERE A.status_onboarding = 'completo' AND A.state = 4

)