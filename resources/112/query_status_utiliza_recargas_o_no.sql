DROP TABLE IF EXISTS `tenpo-bi.tmp.topups_sixmonth{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.topups_sixmonth{{ds_nodash}}` AS 
(

WITH users_ob AS (
  SELECT DISTINCT id AS user
  FROM `tenpo-bi-prod.users.users_tenpo`
  WHERE status_onboarding = 'completo'
),
frecuencia as (
  select user, count(distinct trx_id) as conteo
  FROM `tenpo-bi-prod.economics.economics`
  WHERE linea IN ('top_ups') 
  AND fecha >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)
  AND fecha <= CURRENT_DATE() 
  group by user
  having conteo >= 2
),
recargas AS (
  SELECT DISTINCT e.user as user, 'utiliza_recarga' as ob, f.conteo
  FROM `tenpo-bi-prod.economics.economics` e
  left join frecuencia f on f.user = e.user
  WHERE e.linea IN ('top_ups')
    AND e.linea NOT LIKE '%PFM%'
    AND e.nombre NOT LIKE '%Home%'
    AND e.linea != 'Saldo'
    AND e.linea != 'saldo'
    AND LOWER(e.nombre) NOT LIKE '%devoluc%'
    AND e.linea <> 'reward'
    AND fecha >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 6 MONTH)
  AND fecha <= CURRENT_DATE()
)
SELECT um.user as identity, 
       CASE WHEN r.user IS NULL THEN 'no_utiliza_recarga' ELSE r.ob END AS status_utiliza_recargas
FROM users_ob um
LEFT JOIN recargas r ON r.user = um.user

)
