
DROP TABLE IF EXISTS `tenpo-bi.tmp.activa_y_compra_tc_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.activa_y_compra_tc_{{ds_nodash}}` AS 

(
SELECT A.id AS identity,
CASE
  WHEN B.identity IS NOT NULL AND C.identity IS NOT NULL THEN 1
  ELSE 0 END AS activa_tc_y_realiza_primera_compra
FROM `tenpo-bi-prod.users.users_tenpo`  AS A
LEFT JOIN (
  SELECT DISTINCT user_id AS identity
  FROM `tenpo-airflow-prod.credit_card.physical_card_activation`
  WHERE status = 'COMPLETE'
  ) AS B ON A.id = B.identity
LEFT JOIN (
  SELECT DISTINCT user AS identity
  FROM `tenpo-bi-prod.economics.economics`
  WHERE linea IN ('credit_card_physical')
  ) AS C ON A.id = C.identity
WHERE A.status_onboarding = 'completo' AND A.state = 4

)
