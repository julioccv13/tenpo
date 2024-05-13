{{   config(  tags=["daily", "bi"], materialized='table') }}
WITH 
  cupones as (
    SELECT 
        user
        ,redeem_date
        ,objective
        ,campana
        ,coupon
        ,coupon_type
        ,mov_amount_redeem
        ,date_diff(date(redeem_date), date(ob_completed_at), day) as dias_desde_ob
        ,if(objective = 'Adquisición', 'Con Cupon OB', 'Sin Cupon OB') as tipo_cupon
    FROM {{ ref('coupons_trx_redeems') }}  a
    left join {{ ref('users_tenpo') }} b
    on a.user = b.id
    where confirmed
    qualify row_number() over (partition by user order by redeem_date asc) = 1
  ), onboardings as (
      SELECT 
        u.id as user,
        u.churn, 
        IF(DATE(ob_completed_at, "America/Santiago") < '2020-01-01' , '2020-01-01' ,DATE(ob_completed_at, "America/Santiago")) fecha_ob ,
        if(objective = 'Adquisición', 'Con Cupon OB', 'Sin Cupon OB') as cupon_ob
      FROM  {{ ref('users_tenpo') }}  u --   `tenpo-bi-prod.users.users_tenpo` u  
      left join cupones c
      on u.id = c.user
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
        DATE_TRUNC(fecha_ob, MONTH) mes_ob,
        DATE_DIFF(DATE_TRUNC(fecha_ob, MONTH),'2020-01-01' , MONTH) mes_camada,
        DATE_DIFF(  DATE_TRUNC(fecha, MONTH),'2020-01-01' , MONTH) mes_visita,
        churn,
        cupon_ob
      FROM onboardings 
      LEFT JOIN  economics_app USING(user)
      ),
  lapso_tiempo AS (
        SELECT
          *,
          LAG(mes_visita) OVER (PARTITION BY user ORDER BY user, mes_visita, mes_camada, churn, cupon_ob ASC)  lapso
        FROM log_visitas
      ),
  diferencia_tiempo AS (
       SELECT 
         *,
         mes_visita - lapso   as diferencia
       FROM lapso_tiempo 
      ),
all_data as (
    SELECT DISTINCT
      mes_ob,
      mes_camada,
      cupon_ob,
      COUNT (DISTINCT user)  total_ob,
      COUNT (DISTINCT IF( churn = 0 , user, null)) clientes,
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 0, user , null)) mes_0,
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 1, user , null)) mes_1, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 2, user , null)) mes_2, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 3, user , null)) mes_3, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 4, user , null)) mes_4, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 5, user , null)) mes_5, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 6, user , null)) mes_6, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 7, user , null)) mes_7, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 8, user , null)) mes_8, 
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 9, user , null)) mes_9,
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 10, user , null)) mes_10,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 11, user , null)) mes_11,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 12, user , null)) mes_12,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 13, user , null)) mes_13,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 14, user , null)) mes_14,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 15, user , null)) mes_15,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 16, user , null)) mes_16,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 17, user , null)) mes_17,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 18, user , null)) mes_18,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 19, user , null)) mes_19,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 20, user , null)) mes_20,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 21, user , null)) mes_21,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 22, user , null)) mes_22,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 23, user , null)) mes_23,  
      COUNT( DISTINCT IF(mes_visita - mes_camada <= 24, user , null)) mes_24,  

    FROM diferencia_tiempo 
    WHERE 
      mes_camada is not null
    GROUP BY 
      1,2,3


)

SELECT
  *,
  IF(ROUND(SAFE_DIVIDE(mes_0,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_0,clientes), 4)) mes0_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_1,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_1,clientes), 4)) mes1_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_2,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_2,clientes), 4)) mes2_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_3,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_3,clientes), 4)) mes3_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_4,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_4,clientes), 4)) mes4_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_5,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_5,clientes), 4)) mes5_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_6,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_6,clientes), 4)) mes6_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_7,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_7,clientes), 4)) mes7_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_8,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_8,clientes), 4)) mes8_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_9,clientes), 4) >1,1,ROUND(SAFE_DIVIDE(mes_9,clientes), 4)) mes9_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_10,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_10,clientes),4)) mes10_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_11,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_11,clientes),4)) mes11_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_12,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_12,clientes),4)) mes12_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_13,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_13,clientes),4)) mes13_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_14,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_14,clientes),4)) mes14_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_15,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_15,clientes),4)) mes15_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_16,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_16,clientes),4)) mes16_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_17,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_17,clientes),4)) mes17_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_18,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_18,clientes),4)) mes18_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_19,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_19,clientes),4)) mes19_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_20,clientes),4) >1,1,ROUND(SAFE_DIVIDE(mes_20,clientes),4)) mes20_clientes,
  IF(ROUND(SAFE_DIVIDE(mes_0,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_0,total_ob), 4)) mes0_ob,
  IF(ROUND(SAFE_DIVIDE(mes_1,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_1,total_ob), 4)) mes1_ob,
  IF(ROUND(SAFE_DIVIDE(mes_2,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_2,total_ob), 4)) mes2_ob,
  IF(ROUND(SAFE_DIVIDE(mes_3,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_3,total_ob), 4)) mes3_ob,
  IF(ROUND(SAFE_DIVIDE(mes_4,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_4,total_ob), 4)) mes4_ob,
  IF(ROUND(SAFE_DIVIDE(mes_5,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_5,total_ob), 4)) mes5_ob,
  IF(ROUND(SAFE_DIVIDE(mes_6,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_6,total_ob), 4)) mes6_ob,
  IF(ROUND(SAFE_DIVIDE(mes_7,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_7,total_ob), 4)) mes7_ob,
  IF(ROUND(SAFE_DIVIDE(mes_8,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_8,total_ob), 4)) mes8_ob,
  IF(ROUND(SAFE_DIVIDE(mes_9,total_ob), 4) >1,1,ROUND(SAFE_DIVIDE(mes_9,total_ob), 4)) mes9_ob,
  IF(ROUND(SAFE_DIVIDE(mes_10,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_10,total_ob),4)) mes10_ob,
  IF(ROUND(SAFE_DIVIDE(mes_11,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_11,total_ob),4)) mes11_ob,
  IF(ROUND(SAFE_DIVIDE(mes_12,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_12,total_ob),4)) mes12_ob,
  IF(ROUND(SAFE_DIVIDE(mes_13,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_13,total_ob),4)) mes13_ob,
  IF(ROUND(SAFE_DIVIDE(mes_14,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_14,total_ob),4)) mes14_ob,
  IF(ROUND(SAFE_DIVIDE(mes_15,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_15,total_ob),4)) mes15_ob,
  IF(ROUND(SAFE_DIVIDE(mes_16,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_16,total_ob),4)) mes16_ob,
  IF(ROUND(SAFE_DIVIDE(mes_17,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_17,total_ob),4)) mes17_ob,
  IF(ROUND(SAFE_DIVIDE(mes_18,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_18,total_ob),4)) mes18_ob,
  IF(ROUND(SAFE_DIVIDE(mes_19,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_19,total_ob),4)) mes19_ob,
  IF(ROUND(SAFE_DIVIDE(mes_20,total_ob),4) >1,1,ROUND(SAFE_DIVIDE(mes_20,total_ob),4)) mes20_ob,
FROM all_data
ORDER BY mes_ob desc


