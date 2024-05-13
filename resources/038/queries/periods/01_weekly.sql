-- Inyeccion de parametros desde Airflow
DROP TABLE IF EXISTS `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_003_Params`;
CREATE TABLE `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_003_Params` AS (
    WITH airflow_params AS (
        SELECT 
          CAST(DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 1 WEEK) AS DATETIME) AS Fecha_Ejec
          ,3 AS UTC_CORRECTION
    ), processed_params AS (
        SELECT
            Fecha_Ejec
            ,DATETIME_TRUNC(Fecha_Ejec, WEEK) AS Fecha_Inicio_Ejec
            ,DATETIME_SUB(DATETIME_TRUNC(Fecha_Ejec, WEEK), INTERVAL 1 WEEK) AS Fecha_Inicio_Analisis
            ,DATETIME_SUB(DATETIME_TRUNC(Fecha_Ejec, WEEK), INTERVAL 1 SECOND) AS Fecha_Fin_Analisis

            ,EXTRACT(YEAR FROM Fecha_Ejec)*100 + EXTRACT(MONTH FROM Fecha_Ejec) AS Anomes_Ejec
            ,EXTRACT(YEAR FROM Fecha_Ejec)*100 + EXTRACT(WEEK FROM Fecha_Ejec) AS Semana_Ejec
            ,EXTRACT(YEAR FROM Fecha_Ejec)*12 + EXTRACT(MONTH FROM Fecha_Ejec) AS Mes_Ejec
            
            FROM airflow_params
    )
    SELECT
        p.*
        ,"weekly" periodo
        ,CAST(Fecha_Inicio_Ejec AS DATE) as Fecha_Inicio_Ejec_DT
        ,CAST(Fecha_Inicio_Analisis AS DATE)as Fecha_Inicio_Analisis_DT
        ,CAST(Fecha_Fin_Analisis AS DATE) as Fecha_Fin_Analisis_DT

        ,TIMESTAMP_ADD(CAST(Fecha_Inicio_Ejec AS TIMESTAMP), INTERVAL 3*60 MINUTE) as Fecha_Inicio_Ejec_TS
        ,TIMESTAMP_ADD(CAST(Fecha_Inicio_Analisis AS TIMESTAMP), INTERVAL 3*60 MINUTE) as Fecha_Inicio_Analisis_TS
        ,TIMESTAMP_ADD(CAST(Fecha_Fin_Analisis AS TIMESTAMP), INTERVAL 3*60 MINUTE) as Fecha_Fin_Analisis_TS

        ,EXTRACT(YEAR FROM Fecha_Inicio_Analisis)*100 + EXTRACT(MONTH FROM Fecha_Inicio_Analisis) AS Anomes_Inicio_Analisis
        ,EXTRACT(YEAR FROM Fecha_Inicio_Analisis)*100 + EXTRACT(WEEK FROM Fecha_Inicio_Analisis) AS Semana_Inicio_Analisis
        ,EXTRACT(YEAR FROM Fecha_Inicio_Analisis)*12 + EXTRACT(MONTH FROM Fecha_Inicio_Analisis) AS Mes_Inicio_Analisis

        ,UNIX_SECONDS(TIMESTAMP_ADD(CAST(Fecha_Inicio_Ejec AS TIMESTAMP), INTERVAL 3*60 MINUTE)) as inicio_ejec_desde_epoch
        ,UNIX_SECONDS(TIMESTAMP_ADD(CAST(Fecha_Fin_Analisis AS TIMESTAMP), INTERVAL 3*60 MINUTE)) as cierre_analisis_desde_epoch

    FROM processed_params p
);

/*********************
* Funciones auxiliares
***********************/
CREATE OR REPLACE FUNCTION `${project_target}.aux_table.FILTER_WARDS`(arr ANY TYPE) AS (
    (SELECT ARRAY(
      SELECT 
        x
      FROM UNNEST(arr) AS x
      WHERE x <> '__WARD__'
    ))
);

CREATE OR REPLACE FUNCTION `${project_target}.aux_table.FILTER_WARDS_DISTINCT`(arr ANY TYPE) AS (
    (SELECT ARRAY(
      SELECT 
        DISTINCT x
      FROM UNNEST(arr) AS x
      WHERE x <> '__WARD__'
    ))
);

CREATE OR REPLACE FUNCTION `${project_target}.aux_table.FILTER_NULLS`(arr ANY TYPE) AS (
    (SELECT ARRAY(
      SELECT 
        DISTINCT x
      FROM UNNEST(arr) AS x
      WHERE x is not null
    ))
);

CREATE OR REPLACE FUNCTION `${project_target}.aux_table.DISTINCT_COUNT`(arr ANY TYPE) AS (
    (
      SELECT 
        COUNT(DISTINCT x) 
      FROM UNNEST(arr) AS x
      WHERE x <> '__WARD__'
    )
);

CREATE OR REPLACE FUNCTION `${project_target}.aux_table.ARRAY_MEDIAN_WARD`(arr ANY TYPE) AS (
    (SELECT 
    fhoffa.x.median(ARRAY(
      SELECT 
        x
      FROM UNNEST(arr) AS x
      WHERE x <> -999999999999
    )))
);

CREATE OR REPLACE FUNCTION `${project_target}.aux_table.GET_ANOMES`(dt ANY TYPE) AS (
    (SELECT EXTRACT(YEAR FROM dt)*100 + EXTRACT(MONTH FROM dt))
);