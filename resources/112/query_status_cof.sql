DROP TABLE IF EXISTS `tenpo-bi.tmp.status_cof_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_cof_{{ds_nodash}}` AS 
(

WITH users_ob AS (
  SELECT DISTINCT id AS user
  FROM `tenpo-bi-prod.users.users_tenpo`
  WHERE status_onboarding = 'completo'
),
suscripcion AS (
  SELECT DISTINCT e.user as user, 'registra_cof' as ob
  FROM `tenpo-bi-prod.economics.economics` e 
  WHERE e.nombre in ('Suscripcion') 
  AND fecha >= DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH) - INTERVAL 3 MONTH, MONTH)
  AND fecha < DATE_TRUNC(DATE_TRUNC(CURRENT_DATE(), MONTH), MONTH)
)
SELECT distinct um.user as identity, 
       CASE WHEN s.user IS NULL THEN 'no_registra_cof' ELSE s.ob END AS status_utiliza_cof_tres_meses
FROM users_ob um
LEFT JOIN suscripcion s ON s.user = um.user
ORDER BY um.user 

)
