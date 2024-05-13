/*

  project_id: tenpo-bi

*/

DROP TABLE IF EXISTS `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params`;
CREATE TABLE  `{{project_id}}.temp.DAUMB_{{ds_nodash}}_0003_Params` AS (
    WITH airflow_params AS (
        SELECT
          PARSE_DATE("%Y%m%d", "{{ds_nodash}}") AS Fecha_Analisis
          ,DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 0 DAY) AS Fecha_Ejec
          ,3 AS UTC_CORRECTION
    )

    SELECT 
      *
    FROM airflow_params
);