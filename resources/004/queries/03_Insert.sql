------------------------------------
-- Cuentas
------------------------------------
-- Creates just in case
CREATE TABLE IF NOT EXISTS `${project_target}.active_buyers.active_buyers_equation` 
PARTITION BY Fecha_Inicio_Ejec_DT
AS (
    SELECT * 
    FROM `${project_source}.temp.PAB_{{ds_nodash}}_102_Temp`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM  `${project_target}.active_buyers.active_buyers_equation` 
WHERE Fecha_Inicio_Ejec_DT = (SELECT Fecha_Inicio_Ejec_DT FROM `${project_source}.temp.PAB_{{ds_nodash}}_003_Params`);

INSERT INTO  `${project_target}.active_buyers.active_buyers_equation` 
SELECT * FROM `${project_source}.temp.PAB_{{ds_nodash}}_102_Temp`;

