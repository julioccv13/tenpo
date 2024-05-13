-- Inyeccion de parametros desde Airflow
DROP TABLE IF EXISTS `${project_target}.temp.TPCM_{{ds_nodash}}_0003_Params`;
CREATE TABLE `${project_target}.temp.TPCM_{{ds_nodash}}_0003_Params` AS (
    WITH airflow_params AS (
        SELECT
          DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 1 MONTH) AS Fecha_Ejec
          ,3 AS UTC_CORRECTION
    )

    SELECT 
      *,
      DATE_SUB(Fecha_Ejec, INTERVAL 1 DAY) Fecha_Analisis
    FROM airflow_params
);