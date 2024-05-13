DROP TABLE IF EXISTS `tenpo-bi.tmp.status_auto{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_auto{{ds_nodash}}` AS 
(
  
with users_ob as (
  SELECT DISTINCT ob.id AS user, DATE_DIFF(CURRENT_DATE(), DATE(ob.ob_completed_at), MONTH) as meses_ob
  FROM `tenpo-bi-prod.users.users_tenpo` as ob
  where ob.status_onboarding = 'completo'
),
vehiculos as (
select distinct ID_USUARIO as user, CASE WHEN N_VEHICULOS > 0 THEN 'auto' else 'sin_auto' end as vehiculo
from `tenpo-sandbox.riesgo_backup.Enriquecimiento_Data`
)
SELECT DISTINCT ob.user as identity,
CASE WHEN v.vehiculo = 'auto' THEN 'con_auto' ELSE 'sin_auto' END status_user_vehiculo
from users_ob ob
left join vehiculos v on v.user = ob.user
order by ob.user

)
