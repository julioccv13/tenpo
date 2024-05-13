DROP TABLE IF EXISTS `${project_target}.tmp.journey_cliente_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.journey_cliente_{{ds_nodash}}` AS (
with tabla_final as 
(
    select identity, journey as journey_cliente
    from `${project_source_3}.crm.Productivo_Framework_Fuga`
    where fecha_ejecucion = date(current_date())
)
select distinct
      T1.id as identity,
      IFNULL(T2.journey_cliente,'cliente fuera journey') as journey_cliente
from `${project_source_1}.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.identity
where status_onboarding = 'completo'
)