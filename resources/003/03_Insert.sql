------------------------------------
-- Cuentas
------------------------------------
-- Creates just in case
CREATE TABLE IF NOT EXISTS `{{project_id}}.productos_tenpo.tenencia_productos_tenpo` 
PARTITION BY Fecha_Fin_Analisis_DT
CLUSTER BY user
AS (
    SELECT * 
    FROM `{{project_id}}.temp.TPT_{{ds_nodash}}_102_Temp`
    LIMIT 0
);

-- Idempotent insert
DELETE FROM  `{{project_id}}.productos_tenpo.tenencia_productos_tenpo` 
WHERE Fecha_Fin_Analisis_DT = (SELECT Fecha_Fin_Analisis_DT FROM `{{project_id}}.temp.TPT_{{ds_nodash}}_003_Params`);

INSERT INTO  `{{project_id}}.productos_tenpo.tenencia_productos_tenpo` 
SELECT * FROM `{{project_id}}.temp.TPT_{{ds_nodash}}_102_Temp`;