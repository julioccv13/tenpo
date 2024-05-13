{{  config( materialized='table'  ) }}

WITH
  datos_onboarding as (
      SELECT
        id user,
        DATE(ob_completed_at, "America/Santiago") fecha_ob,
        DATETIME(ob_completed_at, "America/Santiago") timestamp_ob,
        state
      FROM {{ ref('users_tenpo') }} 
      WHERE
      state in (4,7,8,21,22)
      ),
   economics_app as (
   
   SELECT DISTINCT
    user,
    FORMAT_DATE('%Y-%m-01', fecha) mes_trx,
    FROM(
     SELECT DISTINCT
       user,
       trx_id,
       fecha
     FROM {{ ref('economics') }} 
     WHERE 
      linea not in ('reward')
      and nombre not like '%Devolución%'
     ) 
    ),
   
   trx_por_mes as(
     SELECT DISTINCT 
       e.user, 
       e.mes_trx, 
       DATE_DIFF(CAST(ec.mes_trx AS DATE), CAST(e.mes_trx AS DATE),  MONTH) delta_mes,
       COUNT(1) cuenta
     FROM economics_app e
     JOIN datos_onboarding u on u.user = e.user
     JOIN economics_app ec on e.user = ec.user AND e.mes_trx <= ec.mes_trx 
     GROUP BY 1,2,3
       ),
       
    categorizacion_clientes AS(
      SELECT
        user,
        mes_trx,
        delta_mes,
      CASE
        WHEN delta_mes = 0 THEN 'mau_mismo_mes'
        WHEN delta_mes = 1 THEN 'mau_mes_anterior'
        WHEN delta_mes > 1 THEN 'mau_otro_mes'
        WHEN delta_mes IS NULL THEN 'nuevo'
      END AS cust_type
      FROM trx_por_mes
    )
SELECT
  *,
  ROUND(SAFE_DIVIDE(mes_1,maus_mes), 4) mes1_maus,
  ROUND(SAFE_DIVIDE(mes_2,maus_mes), 4) mes2_maus,
  ROUND(SAFE_DIVIDE(mes_3,maus_mes), 4) mes3_maus,
  ROUND(SAFE_DIVIDE(mes_4,maus_mes), 4) mes4_maus,
  ROUND(SAFE_DIVIDE(mes_5,maus_mes), 4) mes5_maus,
  ROUND(SAFE_DIVIDE(mes_6,maus_mes), 4) mes6_maus,
  ROUND(SAFE_DIVIDE(mes_7,maus_mes), 4) mes7_maus,
  ROUND(SAFE_DIVIDE(mes_8,maus_mes), 4) mes8_maus,
FROM(
    SELECT 
     mes_trx,
     COUNT (DISTINCT user) maus_mes,
     COUNT( DISTINCT IF(delta_mes = 1, user , null)) mes_1, 
     COUNT( DISTINCT IF(delta_mes = 2, user , null)) mes_2, 
     COUNT( DISTINCT IF(delta_mes = 3, user , null)) mes_3, 
     COUNT( DISTINCT IF(delta_mes = 4, user , null)) mes_4, 
     COUNT( DISTINCT IF(delta_mes = 5, user , null)) mes_5, 
     COUNT( DISTINCT IF(delta_mes = 6, user , null)) mes_6, 
     COUNT( DISTINCT IF(delta_mes = 7, user , null)) mes_7, 
     COUNT( DISTINCT IF(delta_mes = 8, user , null)) mes_8, 
    FROM trx_por_mes
    GROUP BY 1
    ORDER BY 1 DESC
  )