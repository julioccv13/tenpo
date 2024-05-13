------------------------------------
-- Cuentas
------------------------------------
-- Creates just in case
CREATE TABLE IF NOT EXISTS `${project_target}.tarjeta.activacion_tarjeta_{{period}}` 
PARTITION BY Fecha_Fin_Analisis_DT
CLUSTER BY user
AS (
    SELECT * 
    FROM `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_102_Temp`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM  `${project_target}.tarjeta.activacion_tarjeta_{{period}}` 
WHERE Fecha_Fin_Analisis_DT = (SELECT Fecha_Fin_Analisis_DT FROM `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_003_Params`);

INSERT INTO  `${project_target}.tarjeta.activacion_tarjeta_{{period}}` 
SELECT * FROM `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_102_Temp`;