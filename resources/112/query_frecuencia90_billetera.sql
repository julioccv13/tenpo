DROP TABLE IF EXISTS `tenpo-bi.tmp.frecuencia90_billetera_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.frecuencia90_billetera_{{ds_nodash}}` AS 
(

with users_ob as (
SELECT DISTINCT id AS user
FROM `tenpo-bi-prod.users.users_tenpo` 
WHERE status_onboarding = 'completo'
),
variable as (

with ppabono as (
select distinct b.user as identity,

case when b.user is null then null else a.value end as q_ppabono
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'frecuencia90_ppabono'
group by b.user,a.value

),
ppretiro as (
select distinct b.user as identity,
case when b.user is null then null else a.value end as q_ppretiro
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'frecuencia90_ppretiro'
),
pivot_final as (

select distinct a.identity,
CASE 
  WHEN q_ppabono = 0 OR q_ppretiro = 0 THEN 'N/A'
  ELSE CAST(CAST(q_ppabono AS INT64) + CAST(q_ppretiro AS INT64) AS STRING)
END as frecuencia90_billetera
from ppabono a 
left join ppretiro b on a.identity = b.identity
group by a.identity, q_ppabono, q_ppretiro
)
select identity,frecuencia90_billetera
from pivot_final

)

select a.user as identity, CASE WHEN b.identity is not null then b.frecuencia90_billetera else 'N/A' end as frecuencia90_billetera
from users_ob a 
left join variable b on b.identity = a.user


)
