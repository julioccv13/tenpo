DROP TABLE IF EXISTS `tenpo-bi.tmp.sugpcuentas_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.sugpcuentas_{{ds_nodash}}` AS 
(
  
WITH 
users_ob AS (
  SELECT DISTINCT id AS user
  FROM `tenpo-bi-prod.users.users_tenpo`
  WHERE status_onboarding = 'completo'
),
utilities AS (
  SELECT DISTINCT id AS id, name AS utility
  FROM `tenpo-airflow-prod.tenpo_utility_payment.utilities`
),
bloqueados as (
  SELECT id as user
  FROM tenpo-it-analytics.sac.lock_user
  JOIN tenpo-airflow-prod.users.users ON mail_lock_user = email
),
user_accounts AS (
  SELECT
    uo.user AS user,
    INITCAP(u.utility) AS cuenta_spc,
    case when s.status = 'ENABLED' then CONCAT('$', FORMAT('%\'d', CAST(s.amount AS INT64)))
    when s.status = 'DISABLED' or null then '-1'
    else 'N/A' 
    end as monto_spc,
    ROW_NUMBER() OVER (PARTITION BY uo.user ORDER BY s.amount DESC) AS row_num
  FROM users_ob AS uo
  LEFT JOIN `tenpo-airflow-prod.tenpo_utility_payment.suggestions` AS s ON uo.user = s.user
  LEFT JOIN utilities AS u ON u.id = s.utility_id

  )

  SELECT 
  user as identity,
  cuenta_spc,
  monto_spc
  FROM user_accounts 
  WHERE row_num <= 1 AND user not in (select user from bloqueados) and cuenta_spc is not null
  ORDER BY user, row_num
)
