DROP TABLE IF EXISTS `tenpo-bi.tmp.tipo_usuario_business_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.tipo_usuario_business_{{ds_nodash}}` AS (

with business_pre as 
(select distinct tenpo_user_id as identity, type as usuario_business
from `tenpo-airflow-prod.business.onboarding` 
where status = 'COMPLETED'
),
business as 
(
  select identity, STRING_AGG(usuario_business, ",") as tipo_usuario_business
  from business_pre 
  group by 1
)
select distinct
      T1.id as identity,
      IFNULL(cast(T2.tipo_usuario_business as string),'no es business') as tipo_usuario_business
from `tenpo-bi-prod.users.users_tenpo` T1
  left join business T2 on T1.id = T2.identity
where status_onboarding = 'completo'
)