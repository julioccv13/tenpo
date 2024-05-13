DROP TABLE IF EXISTS `${project_target}.tmp.ob_completo_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.ob_completo_{{ds_nodash}}` AS (
with Tabla_Final as (

select
      id as identity, 'ob_completo' as ob_completado_exitosamente
from `${project_source_1}.users.users_tenpo`
where status_onboarding = 'completo'
)

SELECT
*
from Tabla_Final
);
