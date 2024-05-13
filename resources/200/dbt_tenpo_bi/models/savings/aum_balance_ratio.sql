{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}

WITH 
  saldo_tenpo as (
    SELECT
      fecha
      ,sum(saldo_dia) saldo_total
    FROM {{ ref('daily_balance') }} --`tenpo-bi-prod.balance.daily_balance` 
    GROUP BY 
      1
      ),
  aum as (
    SELECT 
      Fecha_Analisis fecha
      ,SUM( amount ) aum_dia
    FROM {{source('bolsillo','daily_aum')}}  --{{source('bolsillo','daily_aum')}}
    GROUP BY 
      1
      ),
  saldo_clientes_bolsillo as (
   SELECT
      fecha
      ,sum(saldo_dia) saldo_total_clientes_bolsillo
    FROM {{ ref('daily_balance') }} a--`tenpo-bi-prod.balance.daily_balance` 
    JOIN {{source('bolsillo','daily_aum')}} b on fecha = Fecha_analisis and a.user  = b.user --{{source('bolsillo','daily_aum')}} 
    GROUP BY 
      1
      )

SELECT 
  fecha 
  ,aum_dia
  ,saldo_total
  ,saldo_total_clientes_bolsillo
  ,aum_dia/saldo_total as ratio_ob
  ,aum_dia/saldo_total_clientes_bolsillo ratio_clientes_bolsillo
FROM  saldo_tenpo
JOIN aum USING(fecha)
JOIN saldo_clientes_bolsillo USING(fecha)


