{{   config(  tags=["daily", "bi"], materialized='table') }}
WITH 
  onboardings as (
      SELECT 
        u.id as user, 
        u.churn,
        IF(DATE(ob_completed_at, "America/Santiago") < '2020-01-01' , '2020-01-01' ,DATE(ob_completed_at, "America/Santiago")) fecha_ob ,
      FROM  {{ ref('users_tenpo') }}  u --   `tenpo-bi-prod.users.users_tenpo` u  
      WHERE 
        u.state in (4,7,8,21,22)
        AND ob_completed_at is not null
        ),         
  economics_app AS (
      SELECT
        a.* EXCEPT (fecha)
        , IF(fecha < '2020-01-01' , '2020-01-01' , fecha) fecha
        -- , COALESCE(churn, false) churn
      FROM {{ ref('economics') }} a  --{{ ref('economics') }}
      -- LEFT JOIN {{ ref('daily_churn') }}  b on a.user = b.user and a.fecha = b.Fecha_Fin_Analisis_DT --`tenpo-bi-prod.churn.daily_churn` b 
      WHERE
        lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
        AND nombre not like "%Devolución%"
        ),
  log_visitas as(
      SELECT
        user,
        DATE_TRUNC(fecha_ob, ISOWEEK) semana_ob,
        DATE_DIFF(DATE_TRUNC(fecha_ob, ISOWEEK),'2020-01-01' , ISOWEEK) semana_camada,
        DATE_DIFF(  DATE_TRUNC(fecha, ISOWEEK),'2020-01-01' , ISOWEEK) semana_visita,
        churn,
      FROM onboardings 
      LEFT JOIN  economics_app USING(user)
      ),
  lapso_tiempo AS (
        SELECT
          *,
          LAG(semana_visita) OVER (PARTITION BY user ORDER BY user, semana_visita, semana_camada, churn ASC)  lapso
        FROM log_visitas
      ),
  diferencia_tiempo AS (
       SELECT 
         *,
         semana_visita - lapso   as diferencia
       FROM lapso_tiempo 
      ),
all_data as (
    SELECT DISTINCT
      semana_ob,
      semana_camada,
      COUNT (DISTINCT user)  total_ob,
      COUNT (DISTINCT IF( churn = 0 , user, null)) clientes,
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 0, user , null)) semana_0,
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 1, user , null)) semana_1, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 2, user , null)) semana_2, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 3, user , null)) semana_3, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 4, user , null)) semana_4, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 5, user , null)) semana_5, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 6, user , null)) semana_6, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 7, user , null)) semana_7, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 8, user , null)) semana_8, 
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 9, user , null)) semana_9,
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 10, user , null)) semana_10,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 11, user , null)) semana_11,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 12, user , null)) semana_12,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 13, user , null)) semana_13,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 14, user , null)) semana_14,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 15, user , null)) semana_15,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 16, user , null)) semana_16,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 17, user , null)) semana_17,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 18, user , null)) semana_18,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 19, user , null)) semana_19,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 20, user , null)) semana_20,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 21, user , null)) semana_21,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 22, user , null)) semana_22,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 23, user , null)) semana_23,  
      COUNT( DISTINCT IF(semana_visita - semana_camada <= 24, user , null)) semana_24,  

    FROM diferencia_tiempo 
    WHERE 
      semana_camada is not null
    GROUP BY 
      1,2


)

SELECT
  *,
  IF(ROUND(SAFE_DIVIDE(semana_0,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_0,clientes), 4)) semana0_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_1,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_1,clientes), 4)) semana1_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_2,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_2,clientes), 4)) semana2_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_3,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_3,clientes), 4)) semana3_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_4,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_4,clientes), 4)) semana4_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_5,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_5,clientes), 4)) semana5_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_6,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_6,clientes), 4)) semana6_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_7,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_7,clientes), 4)) semana7_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_8,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_8,clientes), 4)) semana8_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_9,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(semana_9,clientes), 4)) semana9_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_10,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_10,clientes),2)) semana10_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_11,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_11,clientes),2)) semana11_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_12,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_12,clientes),2)) semana12_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_13,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_13,clientes),2)) semana13_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_14,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_14,clientes),2)) semana14_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_15,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_15,clientes),2)) semana15_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_16,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_16,clientes),2)) semana16_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_17,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_17,clientes),2)) semana17_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_18,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_18,clientes),2)) semana18_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_19,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_19,clientes),2)) semana19_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_20,clientes),2) >1,1,ROUND(SAFE_DIVIDE(semana_20,clientes),2)) semana20_clientes,
  IF(ROUND(SAFE_DIVIDE(semana_0,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_0,total_ob), 4)) semana0_ob,
  IF(ROUND(SAFE_DIVIDE(semana_1,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_1,total_ob), 4)) semana1_ob,
  IF(ROUND(SAFE_DIVIDE(semana_2,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_2,total_ob), 4)) semana2_ob,
  IF(ROUND(SAFE_DIVIDE(semana_3,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_3,total_ob), 4)) semana3_ob,
  IF(ROUND(SAFE_DIVIDE(semana_4,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_4,total_ob), 4)) semana4_ob,
  IF(ROUND(SAFE_DIVIDE(semana_5,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_5,total_ob), 4)) semana5_ob,
  IF(ROUND(SAFE_DIVIDE(semana_6,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_6,total_ob), 4)) semana6_ob,
  IF(ROUND(SAFE_DIVIDE(semana_7,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_7,total_ob), 4)) semana7_ob,
  IF(ROUND(SAFE_DIVIDE(semana_8,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_8,total_ob), 4)) semana8_ob,
  IF(ROUND(SAFE_DIVIDE(semana_9,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(semana_9,total_ob), 4)) semana9_ob,
  IF(ROUND(SAFE_DIVIDE(semana_10,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_10,total_ob),2)) semana10_ob,
  IF(ROUND(SAFE_DIVIDE(semana_11,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_11,total_ob),2)) semana11_ob,
  IF(ROUND(SAFE_DIVIDE(semana_12,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_12,total_ob),2)) semana12_ob,
  IF(ROUND(SAFE_DIVIDE(semana_13,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_13,total_ob),2)) semana13_ob,
  IF(ROUND(SAFE_DIVIDE(semana_14,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_14,total_ob),2)) semana14_ob,
  IF(ROUND(SAFE_DIVIDE(semana_15,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_15,total_ob),2)) semana15_ob,
  IF(ROUND(SAFE_DIVIDE(semana_16,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_16,total_ob),2)) semana16_ob,
  IF(ROUND(SAFE_DIVIDE(semana_17,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_17,total_ob),2)) semana17_ob,
  IF(ROUND(SAFE_DIVIDE(semana_18,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_18,total_ob),2)) semana18_ob,
  IF(ROUND(SAFE_DIVIDE(semana_19,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_19,total_ob),2)) semana19_ob,
  IF(ROUND(SAFE_DIVIDE(semana_20,total_ob),2) >1,1,ROUND(SAFE_DIVIDE(semana_20,total_ob),2)) semana20_ob,
FROM all_data
ORDER BY semana_ob desc


