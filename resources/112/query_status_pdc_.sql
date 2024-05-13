DROP TABLE IF EXISTS `tenpo-bi.tmp.status_pdc_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_pdc_{{ds_nodash}}` AS 
(

WITH users_ob AS (
  SELECT DISTINCT id AS user
  FROM `tenpo-bi-prod.users.users_tenpo`
  WHERE status_onboarding = 'completo'
),
pago_cuentas AS (
  SELECT DISTINCT e.user as user, 'utiliza_pdc' as ob
  FROM `tenpo-bi-prod.economics.economics` e 
  WHERE e.linea IN ('utility_payments')
    AND e.linea NOT LIKE '%PFM%'
    AND e.nombre NOT LIKE '%Home%'
    AND e.linea != 'Saldo'
    AND e.linea != 'saldo'
    AND LOWER(e.nombre) NOT LIKE '%devoluc%'
    AND e.linea <> 'reward'
)
SELECT um.user as identity, 
       CASE WHEN pc.user IS NULL THEN 'no_utiliza_pc' ELSE pc.ob END AS status_utiliza_pdc_
FROM users_ob um
LEFT JOIN pago_cuentas pc ON pc.user = um.user
ORDER BY um.user

)
