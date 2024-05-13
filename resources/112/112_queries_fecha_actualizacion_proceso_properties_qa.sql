DROP TABLE IF EXISTS `tenpo-bi.tmp.ob_completo_qa_properties_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.ob_completo_qa_properties_{{ds_nodash}}` AS (
with Tabla_Final as (

select
      id as identity, date(current_date('America/Santiago')) as fecha_actualizacion_nuevo_proceso_properties_QA
from `tenpo-bi-prod.users.users_tenpo`
)

SELECT
*
from Tabla_Final
);
