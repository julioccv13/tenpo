DROP TABLE IF EXISTS `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Activity_Aux`;
CREATE TABLE `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Activity_Aux` AS (
WITH
  economics_app AS (

      SELECT
        * EXCEPT(linea),
        CASE 
         WHEN linea in ('aum_savings', 'cash_in_savings', 'cash_out_savings') THEN 'bolsillo'
         WHEN linea like '%p2p%' THEN 'p2p' 
         WHEN linea like '%tyba%' THEN 'tyba' 
         ELSE linea 
         END AS linea
      FROM(
            SELECT
             fecha, nombre, monto, trx_id, user, linea, canal, comercio
            FROM  `${project_source}.economics.economics` 
            LEFT JOIN `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Params` ON 1=1
            WHERE 
              fecha < Fecha_Analisis  
              AND fecha >= DATE_SUB(Fecha_Analisis, INTERVAL 30 DAY)
              AND linea not in ('reward')
              AND nombre not like "%Devolución%"
            UNION ALL
            (SELECT
              fecha, nombre, monto, trx_id, user, linea, canal, comercio
            FROM `${project_source}.economics.economics_p2p_cobro`   p2p
            LEFT JOIN `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Params` ON 1=1
            WHERE 
              fecha < Fecha_Analisis  
              AND fecha >= DATE_SUB(Fecha_Analisis, INTERVAL 30 DAY)
            )
          )
    )

    SELECT
      Fecha_Analisis,
      user, 
      IF(DATE(ob_completed_at, "America/Santiago")<= "2020-01-01", "2020-01-01",DATE(ob_completed_at, "America/Santiago")) fecha_ob, 
      FORMAT_DATE('%Y-%m-01', IF(DATE(ob_completed_at, "America/Santiago")<= "2020-01-01", "2020-01-01",DATE(ob_completed_at, "America/Santiago"))) mes_ob,
      MAX(FORMAT_DATE('%Y-%m-01', fecha)) mes,
      COUNT(DISTINCT if(linea not in('cash_in','cash_out'),linea,null)) uniq_linea,
      COUNT(DISTINCT linea) uniq_linea_cico

    FROM economics_app
    JOIN  `${project_source}.users.users_tenpo` u on user = u.id
    LEFT JOIN `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Params` ON 1=1
    WHERE
      FORMAT_DATE('%Y-%m-01',DATE(ob_completed_at, "America/Santiago")) <= FORMAT_DATE('%Y-%m-01', fecha) 
      AND u.state in (4,7,8,21,22)
    GROUP BY
      Fecha_Analisis, user, ob_completed_at
);

-- Tabla de resultados
CREATE TABLE IF NOT EXISTS `${project_target}.productos_tenpo.tenencia_camada_actividad_mes`
PARTITION BY Fecha_Analisis
CLUSTER BY user
AS (
    SELECT *
    FROM `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Activity_Aux`
    LIMIT 0
);

-- INSERT IDEMPOTENTE
DELETE FROM `${project_target}.productos_tenpo.tenencia_camada_actividad_mes`
WHERE Fecha_Analisis = (SELECT MAX(Fecha_Analisis) FROM `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Activity_Aux`);

INSERT INTO `${project_target}.productos_tenpo.tenencia_camada_actividad_mes`
SELECT * FROM `${project_target}.temp.TPCAM_{{ds_nodash}}_0003_Activity_Aux`;
