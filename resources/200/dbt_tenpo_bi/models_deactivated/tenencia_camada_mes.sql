
{{ config(materialized='table') }}

WITH economics_app AS (
  SELECT
    * EXCEPT(linea),
    CASE WHEN linea like '%p2p%' THEN 'p2p' ELSE linea END AS linea
  FROM (
      SELECT
       fecha,
       nombre,
       monto,
       trx_id,
       user,
       linea,
       canal,
       comercio
      FROM {{ ref('economics') }} 
      WHERE linea in ('mastercard', 'utility_payments', 'top_ups', 'paypal', 'p2p')
      UNION ALL 
      SELECT
       fecha,
       nombre,
       monto,
       trx_id,
       user,
       linea,
       canal,
       comercio
      FROM  {{ ref('economics_p2p_cobro') }}  
      )
  ),

lineas_por_mes as(
  SELECT
    DISTINCT user, 
    IF(DATE(ob_completed_at, "America/Santiago")<= "2020-01-01", "2020-01-01",DATE(ob_completed_at, "America/Santiago")) fecha_ob, 
    FORMAT_DATE('%Y-%m-01', IF(DATE(ob_completed_at, "America/Santiago")<= "2020-01-01", "2020-01-01",DATE(ob_completed_at, "America/Santiago"))) mes_ob,
    FORMAT_DATE('%G%V', fecha) semana,
    FORMAT_DATE('%Y-%m-01', fecha) mes,
    COUNT(DISTINCT linea) uniq_linea
  FROM economics_app
    JOIN  {{ ref('users_tenpo') }}   u on user = u.id
  WHERE 
    FORMAT_DATE('%Y-%m-01',DATE(ob_completed_at, "America/Santiago")) <= FORMAT_DATE('%Y-%m-01', fecha) 
    AND state in (4,7,8,21,22)
  GROUP BY 
    user,fecha_ob, mes,semana, ob_completed_at 
    )
  
 SELECT * FROM lineas_por_mes 

