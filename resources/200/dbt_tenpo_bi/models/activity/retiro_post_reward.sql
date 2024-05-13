{{ config(materialized='table') }}


WITH
  --------------------------------
  --          REWARDS           --
  --------------------------------
  log_rewards as (
    SELECT DISTINCT
      user, 
      DATETIME(trx_timestamp) creacion, 
      linea,
      comercio
    FROM {{ ref('economics') }} 
    WHERE 
      linea in ('reward')
    ),
    
  cuenta_rewards as (
  
    SELECT DISTINCT
      user, 
      count(distinct trx_id) total_rewards
    FROM {{ ref('economics') }} 
    WHERE 
      linea in ('reward') 
    GROUP BY 1
    ),
  
  --------------------------------
  --          CASHOUTS          --
  --------------------------------  
  
  log_cashouts as (
    SELECT DISTINCT
      user, 
      DATETIME(trx_timestamp) creacion, 
      linea,
      comercio
    FROM {{ ref('economics') }} 
    WHERE 
      linea in ('cash_out') 
    ),
  
  log_rewards_cashouts as (  
    SELECT 
      * 
    FROM log_rewards
    
    UNION ALL
    
    SELECT 
      * 
    FROM log_cashouts 
    WHERE 
      user in (SELECT DISTINCT user FROM log_rewards)
      ),
    
    
  retiro_ventana_tiempo as ( 
    SELECT DISTINCT
      * 
    FROM(
      SELECT
        *
      FROM(
        SELECT DISTINCT
          user, 
          linea,
          creacion,
          IF(linea = 'cash_out' AND LAG(linea) OVER (PARTITION BY user ORDER BY creacion) = 'reward' , 
           LAG(comercio) OVER (PARTITION BY user ORDER BY creacion) , null) nombre_premio,
          IF(linea = 'cash_out' AND LAG(linea) OVER (PARTITION BY user ORDER BY creacion) = 'reward' , 
           DATETIME_DIFF(creacion, LAG(creacion) OVER (PARTITION BY user ORDER BY creacion) ,HOUR) , null) dif_tiempo
        FROM log_rewards_cashouts  
        WHERE 
          TRUE = TRUE
        ) 
      WHERE dif_tiempo is not null
      )
--       WHERE 
--         dif_tiempo <= tiempo
        ),
    descriptivos as (
      SELECT DISTINCT
        user,
        AVG(dif_tiempo) media_diftiempo_retiropostreward,
        STDDEV(dif_tiempo) std_diftiempo_retiropostreward,
        COUNT(1) cuenta_retiropostreward
      FROM retiro_ventana_tiempo
      GROUP BY 1
    
    )

SELECT
  *,
  SAFE_DIVIDE( cuenta_retiropostreward, total_rewards) cociente_reward_retiro
FROM(
    SELECT DISTINCT
      user,
      PERCENTILE_CONT(dif_tiempo, 0.5) OVER(PARTITION BY user) AS mediana_diftiempo_retiropostcashout,
      descriptivos.media_diftiempo_retiropostreward ,
      descriptivos.std_diftiempo_retiropostreward ,
      descriptivos.cuenta_retiropostreward,
      total_rewards,  
    FROM retiro_ventana_tiempo 
    LEFT JOIN descriptivos USING(user)
    LEFT JOIN cuenta_rewards USING(user)
    )

