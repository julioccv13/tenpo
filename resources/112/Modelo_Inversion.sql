DROP TABLE IF EXISTS `${project_target}.tmp.Modelo_Inversion_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Modelo_Inversion_{{ds_nodash}}` AS (
with
T1 as (select user as identity, round(y_test_prob,2) as Prob_Inversion, case when y_test_pred = 1 then 'Alta' else 'Baja' end as Tramo_Prob_Inversion from `${project_source_3}.jarvis.PERFIL_INVERSION_PREDICCION` 
where fecha_ejecucion = (select max(fecha_ejecucion) from `${project_source_3}.jarvis.PERFIL_INVERSION_PREDICCION` ))
,tabla_final as (select identity, Prob_Inversion, Tramo_Prob_Inversion from T1)

SELECT
*
from tabla_final
);