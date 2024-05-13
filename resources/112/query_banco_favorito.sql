
DROP TABLE IF EXISTS `tenpo-bi.tmp.banco_favorito_cashinout_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.banco_favorito_cashinout_{{ds_nodash}}` AS 
(

WITH clientes_bancos_principales_cashout AS (
  SELECT distinct
    user,
    CASE 
      WHEN cico_banco_tienda_origen = 'BCI/TBANC/NOVA' and cuenta_destino LIKE '00000000777%' THEN 'MACH'
      ELSE cico_banco_tienda_origen 
      END AS cico_banco_tienda_origen,
    COUNT(*) q_trx
  FROM `tenpo-bi-prod.bancarization.cca_cico_tef` 
  WHERE tipo_trx = 'cashin'
    AND trx_mismo_rut IS TRUE
    AND CAST(ts_trx AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
  GROUP BY 1,2
)
, clientes_bancos_principales_cashin AS (
  SELECT distinct
    user,
    CASE 
      WHEN cico_banco_tienda_origen = 'BCI/TBANC/NOVA' and cuenta_destino LIKE '00000000777%' THEN 'MACH'
      ELSE cico_banco_tienda_origen 
      END AS cico_banco_tienda_origen,
    COUNT(*) q_trx
  FROM `tenpo-bi-prod.bancarization.cca_cico_tef` 
  WHERE tipo_trx = 'cashin'
    AND trx_mismo_rut IS TRUE
    AND CAST(ts_trx AS DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) AND CURRENT_DATE()
  GROUP BY 1,2
)
, cliente_banco_favorito AS (
  SELECT A.identity, A.banco_favorito
  FROM (SELECT A.*
    , ROW_NUMBER() OVER(PARTITION BY identity ORDER BY n_trx DESC) AS rnk
    FROM (SELECT A.user AS identity
      , IFNULL(CAST(A.cico_banco_tienda_origen AS STRING),B.cico_banco_tienda_origen) AS banco_favorito
      , (IFNULL(CAST(A.q_trx AS INT64),0)+ IFNULL(CAST(B.q_trx AS INT64),0)) AS n_trx
      FROM clientes_bancos_principales_cashin AS A
      INNER JOIN clientes_bancos_principales_cashout AS B ON a.user = B.user AND A.cico_banco_tienda_origen = B.cico_banco_tienda_origen
    ) AS A
  ) AS A
  WHERE A.rnk = 1
)

SELECT *
FROM cliente_banco_favorito AS A

)