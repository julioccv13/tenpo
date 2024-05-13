DROP TABLE IF EXISTS `tenpo-bi.tmp.recencia_billetera_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.recencia_billetera_{{ds_nodash}}` AS 
(

with users_ob as (
SELECT DISTINCT id AS user
FROM `tenpo-bi-prod.users.users_tenpo` 
WHERE status_onboarding = 'completo'
),
variable as (

with ppabono as (
select distinct b.user as identity,
CASE when a.value = 0 then null ELSE a.value END as q_ppabono
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'recencia_ppabono_d'
group by b.user,a.value

),
ppretiro as (
select distinct b.user as identity,
CASE when a.value = 0 then null ELSE a.value END as q_ppretiro
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'recencia_ppretiro_d'
),
tabla_combi as (

select identity from ppabono
UNION ALL 
select identity from ppretiro
)
select b.identity, least(COALESCE(b.q_ppabono, 0), COALESCE(c.q_ppretiro, 0)) AS  valor_min
from tabla_combi a
LEFT JOIN ppabono b on a.identity = b.identity
LEFT JOIN ppretiro c on a.identity = c.identity
)
select distinct a.user as identity, 
CASE WHEN b.identity is not null then CAST(b.valor_min AS STRING) else 'N/A' end as recencia_billetera_
from users_ob a 
left join variable b on b.identity = a.user



)
