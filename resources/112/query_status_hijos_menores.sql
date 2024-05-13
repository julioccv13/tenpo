DROP TABLE IF EXISTS `tenpo-bi.tmp.status_hijos_menores{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.status_hijos_menores{{ds_nodash}}` AS 
(

with user_ob as (
select distinct id as user from `tenpo-bi-prod.users.users_tenpo`
where status_onboarding = 'completo' 
),
hijos as (
select distinct ID_USUARIO as user
from `tenpo-sandbox.riesgo_backup.Enriquecimiento_Data`
where N_HIJOS_MENORES >0
)
select a.user as identity, CASE WHEN b.user IS NOT NULL THEN 'Tiene_hijos_menor18' WHEN b.user is null then 'Sin_hijos' else 'Otros' end as status_hijos_menores18
from user_ob a
left join hijos b on b.user = a.user

)