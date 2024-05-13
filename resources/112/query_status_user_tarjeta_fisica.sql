DROP TABLE IF EXISTS `tenpo-bi.tmp.status_tf{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_tf{{ds_nodash}}` AS 
(
  
with 
users_ob as (
  SELECT DISTINCT ob.id AS user, DATE_DIFF(CURRENT_DATE(), DATE(ob.ob_completed_at), MONTH) as meses_ob
  FROM `tenpo-bi-prod.users.users_tenpo` as ob
  where ob.status_onboarding = 'completo'),

tarjeta_fisica as (
  select distinct user, CASE WHEN estado_tarjeta = 'ACTIVE' THEN 'Tarjeta_fisica' ELSE 'sin_tarjeta' END AS estado_tarjeta
  from `tenpo-bi-prod.bancarization.tarjetas_usuarios`
  where tipo = 'PHYSICAL' and estado_tarjeta = 'ACTIVE' 
)
SELECT 
DISTINCT ob.user as identity, 
CASE WHEN tf.estado_tarjeta = 'Tarjeta_fisica' THEN 'tf_activa' ELSE 'tf_no_activa' END status_tarjeta_fisica,
from users_ob ob
left join tarjeta_fisica tf on tf.user = ob.user
order by ob.user


)
