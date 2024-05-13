DROP TABLE IF EXISTS `tenpo-bi.tmp.status_int_bencina{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_int_bencina{{ds_nodash}}` AS 
(
  
with BENCINA as (
  select user,count(DISTINCT DATE_TRUNC(date(fecha), MONTH)) as conteo_meses
  from `tenpo-bi-prod.economics.economics`
  where
  (
  lower(comercio) LIKE '%copec%'
    or lower(comercio) LIKE '%muevo%'
    or lower(comercio) LIKE '%shell%'
    or lower(comercio) LIKE '%copiloto%'
    or lower(comercio) LIKE '%petrobras%'
    or lower(comercio) LIKE '%petrob%'
    or lower(comercio) LIKE '%terpel%'
  )
  and fecha BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND CURRENT_DATE()
  group by user,fecha
  order by fecha desc
),
users_ob as (
  SELECT DISTINCT ob.id AS user, DATE_DIFF(CURRENT_DATE(), DATE(ob.ob_completed_at), MONTH) as meses_ob
  FROM `tenpo-bi-prod.users.users_tenpo` as ob
  where ob.status_onboarding = 'completo'
),
users_ob_6m as (
  SELECT DISTINCT ob.id AS user, DATE_DIFF(CURRENT_DATE(), DATE(ob.ob_completed_at), MONTH) as meses_ob
  FROM `tenpo-bi-prod.users.users_tenpo` as ob
  where ob.status_onboarding = 'completo'
  and DATE_DIFF(CURRENT_DATE(), DATE(ob.ob_completed_at), MONTH)>=6
),
mediana as (
  SELECT
    APPROX_QUANTILES(conteo_meses, 2)[OFFSET(1)] as mediana
  FROM BENCINA a
  join users_ob_6m b on a.user=b.user)

SELECT DISTINCT ob.user as identity,
CASE
    WHEN (ob.meses_ob < 6 AND b.conteo_meses >= ob.meses_ob / 2)
      OR (ob.meses_ob >= 6 AND b.conteo_meses >= m.mediana) THEN 'Intensivo'
    ELSE 'No_intensivo' END as status_intensivo_bencina
from users_ob ob
left join BENCINA b on b.user = ob.user
cross join mediana m
order by ob.user


)
