{{ 
  config(
    materialized='table',
  ) 
}}

WITH
   ob_exitoso as (
    SELECT DISTINCT
      fecha_ob,
      count(distinct id) cuenta,
    FROM {{ ref('funnel_bolsillo') }} --`tenpo-bi-prod.funnel.funnel_bolsillo` --CAMBIAR A PROD
    WHERE 
      paso = 'OB Exitoso'
      AND fecha_ob is not null
    GROUP BY 
      1
      ),
    ob_acum as (
      SELECT 
       fecha_ob, 
       SUM(cuenta) OVER (ORDER BY fecha_ob ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ob_acumulado  
      FROM ob_exitoso 
      ),
    aum_cliente as (
    SELECT DISTINCT 
      fecha,
      COUNT(DISTINCT user) clientes_activos
    FROM {{ ref('economics') }} --`tenpo-bi-prod.economics.economics` 
    WHERE 
      linea = 'aum_savings'
    GROUP BY 1
    )
      
      
SELECT 
  a.fecha_ob,
  ob_acumulado,
  clientes_activos,
  SAFE_DIVIDE(clientes_activos,ob_acumulado) ratio_activos  
FROM ob_acum a JOIN aum_cliente b ON fecha = fecha_ob