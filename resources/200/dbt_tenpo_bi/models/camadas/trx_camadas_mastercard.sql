{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  economics_app AS (
    SELECT distinct
      fecha,
      nombre,
      monto,
      trx_id,
      user,
      linea,
      canal,
      comercio
      FROM {{ ref('economics') }} --{{ref('economics')}}  
     WHERE 
       linea  in ("mastercard", "mastercard_physical")
       ),
  trx_por_mes as(
    SELECT 
      user, 
      DATE(ob_completed_at, "America/Santiago") fecha_ob, 
      FORMAT_DATE('%Y-%m-01',DATE(ob_completed_at, "America/Santiago")) mes_ob,
      FORMAT_DATE('%G%V', fecha) semana,
      FORMAT_DATE('%Y-%m-01', fecha) mes,
      SUM(monto) monto_gastado, 
      count( distinct trx_id) cuenta_trx
    FROM economics_app
    JOIN  {{ ref('users_tenpo') }} on id = user  --{{ref('users_tenpo')}}
    WHERE 
      FORMAT_DATE('%Y-%m-01',DATE(ob_completed_at, "America/Santiago")) <= FORMAT_DATE('%Y-%m-01', fecha) 
      AND state in (4,7,8,21,22)
    GROUP BY 
      user,fecha_ob, mes,semana, ob_completed_at 
    ORDER BY
      user, mes , semana DESC
      )
  
 SELECT 
  * 
FROM trx_por_mes 
