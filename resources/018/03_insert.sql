/*

  project_id: tenpo-bi
  project_source_1: tenpo-airflow-prod

*/
-- Tabla de resultados
CREATE TABLE IF NOT EXISTS `{{project_id}}.savings.daily_aum`
PARTITION BY Fecha_Analisis
CLUSTER BY user
AS (
    SELECT *
    FROM `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Activity_Aux`
    LIMIT 0
);

-- INSERT IDEMPOTENTE
DELETE FROM `{{project_id}}.savings.daily_aum`
WHERE Fecha_Analisis = (SELECT MAX(Fecha_Analisis) FROM `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Activity_Aux`);

INSERT INTO `{{project_id}}.savings.daily_aum`
SELECT * FROM `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Activity_Aux`;
