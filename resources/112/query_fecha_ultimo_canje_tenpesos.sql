DROP TABLE IF EXISTS `tenpo-bi.tmp.fecha_ultimo_canje_tenpesos_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.fecha_ultimo_canje_tenpesos_{{ds_nodash}}` AS 
(
  
SELECT 
A.id as identity, 
CASE 
  WHEN B.UlimaFecha_CashOut IS NULL THEN 'N/A' 
  ELSE STRING(B.UlimaFecha_CashOut)
  END AS fecha_ultimo_canje_tenpesos
FROM (SELECT *
  FROM `tenpo-bi-prod.users.users_tenpo` as A
  WHERE A.status_onboarding = 'completo' and A.state = 4
  ) AS A
LEFT JOIN (SELECT user_id, MAX(DATE(created_at)) AS UlimaFecha_CashOut
  FROM `tenpo-airflow-prod.tenpesos_transaction.transactions`
  WHERE transaction_type = 'CASH_OUT'
  GROUP BY user_id
  ) AS B ON A.id = B.user_id
--WHERE B.UlimaFecha_CashOut IS NOT NULL


)
