{{ config(materialized='view') }}

WITH
  data AS(
      WITH

        ---Tabla de clientes PayPal
        clientes_paypal AS (
          SELECT DISTINCT
            rut,
            CASE WHEN COUNT(t2.tip_operacion_multicaja) OVER (PARTITION BY rut) = 1 THEN t2.tip_operacion_multicaja ELSE 'AMBOS' END as tip_op
          FROM
            {{source('paypal','pay_cliente')}}as t1
          JOIN 
            {{source('paypal','pay_cuenta')}} as t2
          ON t1.id = t2.id_cliente 
          ORDER BY 1
          ),

        ---Tabla de clientes Tenpo
        clientes_tenpo AS (
         SELECT DISTINCT
         tributary_identifier as rut,
         state,
         created_at as ts_creacion,
         ob_completed_at 
         FROM
         {{source('tenpo_users','users')}}
         )
         
         --- Genero los datos
         SELECT
         COUNT(1) as cant_users_paypal,
         state,
         tip_op,
         cast (ts_creacion as DATE) as date_creacion,
         ob_completed_at
         FROM clientes_paypal 
         JOIN clientes_tenpo 
         USING (rut)
         group by 2,3,4,5
      )
    
    --- Agrego acumulado
    select 
    *,
    SUM(cant_users_paypal) OVER (PARTITION BY state,tip_op ORDER BY date_creacion ASC) AS acumulado,
    from data
   