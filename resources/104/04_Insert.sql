------------------------------------
-- Cuentas
------------------------------------
-- Creates just in case
CREATE TABLE IF NOT EXISTS `${project_target}.tablones_analisis.tablon_rfmp_paypal` 
PARTITION BY Fecha_Fin_Analisis_DT
CLUSTER BY user
AS (
    SELECT * 
    FROM `${project_target}.temp.P_{{ds_nodash}}_104_Temp`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM `${project_target}.tablones_analisis.tablon_rfmp_paypal` 
WHERE Fecha_Fin_Analisis_DT = (SELECT Fecha_Fin_Analisis_DT FROM `${project_target}.temp.P_{{ds_nodash}}_104_Params`);

INSERT INTO `${project_target}.tablones_analisis.tablon_rfmp_paypal` 
SELECT * FROM `${project_target}.temp.P_{{ds_nodash}}_104_Temp`;
