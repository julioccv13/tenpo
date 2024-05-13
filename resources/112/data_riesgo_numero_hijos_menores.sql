DROP TABLE IF EXISTS `tenpo-bi.tmp.data_riesgo_numero_hijos_menores_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.data_riesgo_numero_hijos_menores_{{ds_nodash}}` AS (

with data_riesgo as 
(
    SELECT ID_USUARIO as identity, cast(max(N_HIJOS_MENORES) as int) as data_riesgo_numero_hijos_menores 
    FROM `tenpo-sandbox.riesgo_backup.Enriquecimiento_Data` 
    group by 1
)
select
      a.id as identity
      ,ifnull(b.data_riesgo_numero_hijos_menores,0) as data_riesgo_numero_hijos_menores
from `tenpo-bi-prod.users.users_tenpo` a
  left join data_riesgo b on a.id = b.identity
where a.status_onboarding = 'completo'
)