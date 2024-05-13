DROP TABLE IF EXISTS `${project_target}.tmp.recomendador_rubro_top1_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.recomendador_rubro_top1_{{ds_nodash}}` AS (
with tabla_final as 
(
    select distinct
    user as identity,
    rubro as recomendador_rubro_top1,
    from `${project_source_3}.jarvis.SISTEMA_RECOMENDADOR_OUTPUT` a
    where
    ranking1='Top1'
    and ranking2='Top1'
)
select distinct
      T1.id as identity,
      IFNULL(T2.recomendador_rubro_top1,'sin recomendacion') as recomendador_rubro_top1
from `${project_source_1}.users.users_tenpo` T1
  left join tabla_final T2 on T1.id = T2.identity
where status_onboarding = 'completo'
)
