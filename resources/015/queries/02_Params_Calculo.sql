
-- Inyeccion de parametros desde Airflow
DROP TABLE IF EXISTS `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Params`;
CREATE TABLE `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Params` AS (
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

DROP TABLE IF EXISTS `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Activity_Aux`;
CREATE TABLE `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Activity_Aux` AS (
WITH
  economics_app AS (

      SELECT DISTINCT
        * EXCEPT(linea),
        CASE 
         WHEN linea like '%p2p%' THEN 'p2p' 
         ELSE linea 
         END AS linea,
      FROM(
            SELECT
             fecha, nombre, monto, trx_id, user, linea, canal, comercio
            FROM  `{project_source_1}.economics.economics` 
            LEFT JOIN `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Params` ON 1=1
            WHERE 
              fecha <= Fecha_Analisis  
              AND fecha >= DATE_SUB(Fecha_Analisis, INTERVAL 29 DAY)
              AND linea not in ('reward')
          )
    )

  SELECT 
    *,
    DATE_DIFF(Fecha_Analisis, last_trx, DAY) recency
  FROM (
    SELECT DISTINCT
      Fecha_Analisis,
      user,
      LAST_VALUE(fecha) OVER (PARTITION BY user ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_trx,
    FROM economics_app
    JOIN  `{project_source_1}.users.users_tenpo` u on user = u.id
    LEFT JOIN  `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Params`  ON 1=1
    WHERE 
      u.state in (4,7,8)
      )
);

-- Tabla de resultados
CREATE TABLE IF NOT EXISTS `{project_id}.mau.daily_mau_app`
PARTITION BY Fecha_Analisis
CLUSTER BY user
AS (
    SELECT *
    FROM `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Activity_Aux`
    LIMIT 0
);

-- INSERT IDEMPOTENTE
DELETE FROM `{project_id}.mau.daily_mau_app`
WHERE Fecha_Analisis = (SELECT MAX(Fecha_Analisis) FROM `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Activity_Aux`);

INSERT INTO `{project_id}.mau.daily_mau_app`
SELECT * FROM `{project_id}.temp.DMAU_{{ds_nodash}}_0003_Activity_Aux`;

