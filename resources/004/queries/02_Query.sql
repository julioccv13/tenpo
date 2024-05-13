DROP TABLE IF EXISTS `${project_source}.temp.PAB_{{ds_nodash}}_102_Temp`;
CREATE TABLE `${project_source}.temp.PAB_{{ds_nodash}}_102_Temp` AS (

WITH
  datos as (
      SELECT 
        fecha_max
        ,Fecha_Inicio_Ejec_DT 
        ,EXTRACT( DAY FROM  fecha_max) dia
        ,total_ab
        ,(total_ab - LAG(total_ab) OVER (PARTITION BY EXTRACT( MONTH FROM  fecha_max) ORDER BY fecha_max ))/ LAG(total_ab) OVER (PARTITION BY EXTRACT( MONTH FROM  fecha_max) ORDER BY fecha_max ) crecim_diario
      FROM `${project_target}.active_buyers.active_buyers_proyectado` 
      LEFT JOIN `${project_source}.temp.PAB_{{ds_nodash}}_003_Params` on 1 = 1
      WHERE 
        fecha_max < Fecha_Inicio_Ejec_DT
        AND fecha_max >= DATE_SUB(Fecha_Inicio_Ejec_DT, INTERVAL 3 MONTH) 
      ),
  log_natural as (
      SELECT 
        Fecha_Inicio_Ejec_DT 
        ,LN(dia) X
        ,LN(ROUND(MIN(crecim_diario),3)) Y
      FROM datos
      WHERE crecim_diario is not null and crecim_diario>=0
      GROUP BY 1,2
      ),
  sumas_de_cuadrados as (
      SELECT 
        Fecha_Inicio_Ejec_DT,
        COUNT(1) AS N,
        SUM(X) AS SUM_OF_X,
        SUM(Y) AS SUM_OF_Y,
        STDDEV_POP(X) AS STDDEV_OF_X,
        STDDEV_POP(Y) AS STDDEV_OF_Y,
        CORR(X,Y) AS CORRELATION
      FROM log_natural
      GROUP BY 1
      ),
  ecuacion as (
      SELECT
        Fecha_Inicio_Ejec_DT,
        N,
        SUM_OF_X,
        SUM_OF_Y,
        CORRELATION * STDDEV_OF_Y / STDDEV_OF_X AS SLOPE,
      FROM sumas_de_cuadrados 
    )
    
SELECT 
  Fecha_Inicio_Ejec_DT,
  SLOPE potencia,
  EXP((SUM_OF_Y - SLOPE * SUM_OF_X) / N) AS beta,
FROM ecuacion

);