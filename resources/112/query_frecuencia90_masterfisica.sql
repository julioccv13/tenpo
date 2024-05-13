DROP TABLE IF EXISTS `tenpo-bi.tmp.query_frecuencia90_masterfisica{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.query_frecuencia90_masterfisica{{ds_nodash}}` AS 
(

with users_ob as (
SELECT DISTINCT id AS user
FROM `tenpo-bi-prod.users.users_tenpo` 
WHERE status_onboarding = 'completo'
),
variable as (

select distinct b.user as identity,
CASE when a.value = 0 then 'N/A' when a.value IS NOT NULL THEN CAST(a.value AS STRING) ELSE 'N/A' END as frecuencia90_mastercard_fisica
from `tenpo-sandbox.crm.FACT_Features` a
left join `tenpo-sandbox.crm.DIM_User` b on a.idUser = b.idUser 
left join `tenpo-sandbox.crm.DIM_Feature` c on a.idFeature = c.idFeature
left join `tenpo-sandbox.crm.DIM_Answer` d on a.idFeature=d.idFeature and a.idAnswer = d.idAnswer 
left join `tenpo-sandbox.crm.DIM_Time` e on a.idTime = e.idTime
where c.codename = 'frecuencia90_masterf'

)

select a.user as identity, CASE WHEN b.identity is not null then b.frecuencia90_mastercard_fisica else 'N/A' end as frecuencia90_mastercard_fisica
from users_ob a 
left join variable b on b.identity = a.user
)
