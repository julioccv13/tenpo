DROP TABLE IF EXISTS `tenpo-bi.tmp.clasf_bencina{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.clasf_bencina{{ds_nodash}}` AS 
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
USER_BENCINA_HIST as (
  select distinct user
  from `tenpo-bi-prod.economics.economics`
  where
  lower(comercio) LIKE '%copec%'
    or lower(comercio) LIKE '%muevo%'
    or lower(comercio) LIKE '%shell%'
    or lower(comercio) LIKE '%copiloto%'
    or lower(comercio) LIKE '%petrobras%'
    or lower(comercio) LIKE '%petrob%'
    or lower(comercio) LIKE '%terpel%'
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
  join users_ob_6m b on a.user=b.user
),
vehiculos as (
select distinct ID_USUARIO as user, CASE WHEN N_VEHICULOS > 0 THEN 'auto' else 'sin_auto' end as vehiculo
from `tenpo-sandbox.riesgo_backup.Enriquecimiento_Data`
),
tarjeta_fisica as (
  select distinct user, CASE WHEN estado_tarjeta = 'ACTIVE' THEN 'Tarjeta_fisica' ELSE 'sin_tarjeta' END AS estado_tarjeta
  from `tenpo-bi-prod.bancarization.tarjetas_usuarios`
  where tipo = 'PHYSICAL' and estado_tarjeta = 'ACTIVE'
)
SELECT DISTINCT ob.user as identity,
  case
    WHEN ob.meses_ob < 6 AND b.conteo_meses >= ob.meses_ob / 2 THEN 'intensivo_bencina' ----usuario que ha estado en tenpo durante menos de 6 meses y ha hecho compras durante al menos la mitad de este tiempo
    WHEN ob.meses_ob >=6 AND b.conteo_meses >= m.mediana THEN 'intensivo_bencina' ---usuario con mas de 6 meses o más y ha hecho trx durante mas meses que la mediana de todos los usuarios
    WHEN hb.user is not null AND tf.estado_tarjeta = 'Tarjeta_fisica' AND v.vehiculo = 'auto' THEN 'no_intensivo_bencina_con-tf_con-auto' 
    WHEN hb.user is not null AND tf.estado_tarjeta = 'sin_tarjeta' AND v.vehiculo = 'sin_auto' THEN 'no_intensivo_bencina_sin-tf_sin-auto'
    WHEN hb.user is not null AND tf.estado_tarjeta = 'sin_tarjeta' AND v.vehiculo = 'auto' THEN 'no_intensivo_bencina_sin-tf_con-auto'
    WHEN hb.user is not null AND tf.estado_tarjeta = 'Tarjeta_fisica' AND v.vehiculo = 'sin_auto' THEN 'no_intensivo_bencina_con-tf_sin-auto'
    ELSE 'sin_trx_bencina'
  END as clasificacion_intensividad_bencina

from users_ob ob
left join BENCINA b on b.user = ob.user
left join USER_BENCINA_HIST hb on hb.user = ob.user
left join tarjeta_fisica tf on tf.user = ob.user
left join vehiculos v on v.user = ob.user
cross join mediana m
order by ob.user

)
