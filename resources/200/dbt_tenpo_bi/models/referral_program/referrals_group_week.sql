{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}
WITH
  parejas as (
    SELECT DISTINCT 
      referrer,
      grupo,
      user,
      camada_ob_referrer,
      IF(fecha_ob_user < fecha_ingreso_beta, fecha_ingreso_beta, fecha_ob_user) fecha_ob_user ,
      fecha_ingreso_beta,
      camada_ob_user
    FROM {{ ref('parejas_iyg') }}
    ),
  ob_mes as(
    SELECT DISTINCT referrer, 
      user,
      grupo,
      COUNT( distinct user) cuenta_ob_iyg,
      fecha_ingreso_beta,
      fecha_ob_user,
      DATE_TRUNC(fecha_ingreso_beta, ISOWEEK) sem_ingreso_beta,
      DATE_TRUNC(fecha_ob_user, ISOWEEK) sem_ob_user,    
    FROM parejas
    WHERE user is not null
    GROUP BY 
      user,referrer, fecha_ob_user, grupo, fecha_ingreso_beta
    ORDER BY  
     referrer DESC
      )
      
SELECT 
  *,
  DATE_DIFF(fecha_ob_user, fecha_ingreso_beta, DAY) dias_diff,
  DATE_DIFF(fecha_ob_user, fecha_ingreso_beta, ISOWEEK) isoweek_diff
FROM ob_mes