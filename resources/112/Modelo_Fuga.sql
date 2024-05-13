DROP TABLE IF EXISTS `${project_target}.tmp.Modelo_Fuga_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.Modelo_Fuga_{{ds_nodash}}` AS (
with 
Tabla_Final as (SELECT user as identity,probabilidad_fuga, case when probabilidad_fuga <=0.45 then 'Baja'
                                      when probabilidad_fuga >0.45 and probabilidad_fuga<= 0.7 then 'Medio'
                                      else 'Alta' end M_Fuga FROM `${project_target}.analytics_models.early_churn_predict_*`
                                      where date(fecha_ob) = (select max(date(fecha_ob)) from `${project_target}.analytics_models.early_churn_predict_*`))
SELECT
*
from Tabla_Final
);
