{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}

WITH
  aum_cliente as (
    SELECT DISTINCT 
      fecha
      ,user
      ,monto monto_posicion
    FROM {{ ref('economics') }}  --{{ref('economics')}} 
    WHERE 
      linea = 'aum_savings'
      )
      
SELECT
  fecha
  ,case 
   when monto_posicion <= 100 then "menos de $100"
   when monto_posicion between 101 and 1000 then "[$101,$1.000]"
   when monto_posicion between 1001 and 10000 then  "[$1.001,$10.000]"
   when monto_posicion > 10000 then "más de $10.000"
   else "otro"
   end as grupo
   ,case 
   when monto_posicion <= 100 then 1
   when monto_posicion between 101 and 1000 then 2
   when monto_posicion between 1001 and 10000 then  3
   when monto_posicion > 10000 then 4
   else 5
   end as num_grupo
   ,count(distinct user) usuarios
 FROM aum_cliente
 GROUP BY 
  1,2,3


