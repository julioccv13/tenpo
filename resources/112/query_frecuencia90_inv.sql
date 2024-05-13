DROP TABLE IF EXISTS `tenpo-bi.tmp.frecuencia90_inv_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.frecuencia90_inv_{{ds_nodash}}` AS 
(

with users_ob as (
SELECT DISTINCT id AS user
FROM `tenpo-bi-prod.users.users_tenpo` 
WHERE status_onboarding = 'completo'
),
variable as (
select distinct b.user as identity,
CASE when a.value = 0 then 'N/A' when a.value IS NOT NULL THEN CAST(a.value AS STRING) ELSE 'N/A' END as frecuencia90_inv
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'frecuencia90_inv'
)

select a.user as identity, CASE WHEN b.identity is not null then b.frecuencia90_inv else 'N/A' end as frecuencia90_inv
from users_ob a 
left join variable b on b.identity = a.user

)
