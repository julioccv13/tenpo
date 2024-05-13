


DROP TABLE IF EXISTS `tenpo-bi.tmp.monto_tenpesos_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.monto_tenpesos_{{ds_nodash}}` AS 
(
  
SELECT 
A.user_id as identity,
LEFT(TRANSLATE(
    FORMAT("%'.2f", CAST(total as NUMERIC)),
    ',.',
    '.,'),LENGTH(TRANSLATE(
    FORMAT("%'.2f", CAST(total as NUMERIC)),
    ',.',
    '.,'))-3) AS Monto_Tenpesos
FROM (SELECT user_id, MAX(last_total_update_date) AS UlimaFecha
  FROM `tenpo-airflow-prod.tenpesos_balances.audit_balance`
  WHERE status = 'ACTIVE'
  GROUP BY user_id) AS A
LEFT JOIN (SELECT *
  FROM `tenpo-airflow-prod.tenpesos_balances.audit_balance` A
  WHERE status = 'ACTIVE') 
  AS B ON A.user_id = B.user_id AND A.UlimaFecha = B.last_total_update_date

)
