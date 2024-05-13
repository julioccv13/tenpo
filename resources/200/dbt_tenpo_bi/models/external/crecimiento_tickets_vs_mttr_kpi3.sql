{{ config( 
    tags=["daily", "bi"],
    materialized='table')
}}

with daily_open_tickets as (
  SELECT
        DATE(hora_inicial) as dia_ticket_eval,      
        ROUND(avg_mttr_day,3) AS avg_mttr_day,
        origen,
        grupo
  FROM {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.mttr_cantidad_tickets_abiertos_daily`' }}
),
daily_tickets_mttr as (
  SELECT DATE(hora_inicial) as dia_ticket_eval,
  cantidad_tickets_abiertos as tickets_abiertos,
  origen,
  grupo
   from {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.cantidad_tickets_abiertos_daily`' }}
)
select b.dia_ticket_eval, b.avg_mttr_day, b.origen, b.grupo, a.tickets_abiertos from daily_tickets_mttr  a
left join daily_open_tickets  b on a.origen=b.origen and a.grupo=b.grupo and a.dia_ticket_eval=b.dia_ticket_eval 
order by dia_ticket_eval desc

