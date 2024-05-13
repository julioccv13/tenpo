DROP TABLE IF EXISTS `tenpo-bi.tmp.orquestador_audiencia_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.orquestador_audiencia_{{ds_nodash}}` AS (
with tabla_final as 
(
    select user, max(audiencia) as audiencia
    from `tenpo-sandbox.crm.Productivo_audiencias_orquestador`
    where fecha_ingreso = (select max(fecha_ingreso) from `tenpo-sandbox.crm.Productivo_audiencias_orquestador`) and excluido = 'no'
    group by 1
)
select distinct
      T1.id as identity,
      IFNULL(T2.audiencia,'cliente no ingresa a audiencia hoy') as orquestador_audiencia
from `tenpo-bi-prod.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.user
where status_onboarding = 'completo'
)
