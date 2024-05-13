{{ config( 
    tags=["daily", "bi"],
    materialized='table')
}}

with daily_mau as (
  SELECT
        Fecha_Analisis,
        count(distinct user)  maus_diarios
  FROM `tenpo-bi.mau.daily_mau_app_v2` 
  group by 1 order by 1 desc
),

daily_tickets as (

  SELECT DATE(hora_inicial) as dia_ticket_eval,
  cantidad_tickets_abiertos as tickets_abiertos,
  origen,
  grupo
   from {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.cantidad_tickets_abiertos_daily`' }}
)

select *, ROUND(tickets_abiertos/a.maus_diarios,3) as ratio from daily_mau a
left join daily_tickets b on a.Fecha_Analisis=b.dia_ticket_eval
order by a.Fecha_Analisis desc