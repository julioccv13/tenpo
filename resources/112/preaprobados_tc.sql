DROP TABLE IF EXISTS `tenpo-bi.tmp.preaprobados_tc_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.preaprobados_tc_{{ds_nodash}}` AS (
select distinct
      a.id as identity
      , ifnull(b.tc_clasificacion_large, 's/i') as tc_clasificacion_large
      , ifnull(b.tc_cupo_preaprobado, 0) as tc_cupo_preaprobado
      , ifnull(b.tc_marca_preaprobado, 's/i') as tc_marca_preaprobado
from `tenpo-bi-prod.users.users_tenpo` a
  left join 
  (
    select distinct id_usuario, clasificacion as tc_clasificacion_large, ifnull(cast(cupo as int),0) as tc_cupo_preaprobado, case when marca_preaprobado = 1 then 'preaprobado' else 'no preaprobado' end as tc_marca_preaprobado 
    from `tenpo-sandbox.riesgo_backup.CLIENTES_EVALUADOS_Rubros`
  ) b on a.id = b.id_usuario
where a.status_onboarding = 'completo'
)
