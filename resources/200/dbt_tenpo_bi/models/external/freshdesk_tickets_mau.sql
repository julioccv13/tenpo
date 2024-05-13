{{ config( 
    tags=["daily", "bi"],
    materialized='table')
}}


WITH 
  maus as (
    SELECT DISTINCT
      DATE_TRUNC(fecha, MONTH) mes,
      COUNT( DISTINCT user) as total_ab,
    FROM{{ ref('economics') }}  
    WHERE TRUE
      AND linea not in ('reward')
    GROUP BY 1
    ORDER BY 1 DESC ),
  tickets as (
    SELECT
      DATE_TRUNC(DATE(creacion), MONTH) mes
      ,COUNT(distinct id) cuenta 
      ,COUNT(distinct user) usuarios
    FROM
      {{ ref('tickets_freshdesk') }} 
    WHERE 
      tipo not in ('Validacion identidad','Cierre de cuenta')
    GROUP BY 
      1

      )
SELECT 
  mes
  ,tickets.cuenta  tickets
  ,total_ab
  ,tickets.cuenta /maus.total_ab ticket_mau
  , tickets.cuenta  / tickets.usuarios  ticket_contacto
FROM tickets 
LEFT JOIN maus USING(mes)
ORDER BY
   1 DESC