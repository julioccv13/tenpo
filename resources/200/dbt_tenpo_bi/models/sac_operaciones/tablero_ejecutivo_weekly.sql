{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='tablero_ejecutivo_weekly'
    )
}}

with table1 as (
select DATE_TRUNC(dia, WEEK) as fecha_mes,
ejecutivo,
sum(llamadas_atendidas) as llamadas_atendidas,
sum(denominador_csat) as denominador_csat,
sum(numerador_csat) as numerador_csat,
sum(denominador_fcr) as denominador_fcr,
sum(numerador_fcr) as numerador_fcr,
ROUND(avg(TMO_min),3) as TMO_min from {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.tablero_ejecutivo`' }}
group by 1, 2 )
select fecha_mes, ejecutivo, llamadas_atendidas,  ROUND(numerador_csat/denominador_csat,3) as csat_semana, ROUND(numerador_fcr/denominador_fcr,3) as fcr_semana,
denominador_fcr as cantidad_EPA1, denominador_csat as cantidad_EPA2, TMO_min
from table1