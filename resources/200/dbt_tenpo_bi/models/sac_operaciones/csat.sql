{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='csat'
    )
}}

with denominador as (
  select DATE(fecha_larga) as dia,  count(*) as total FROM {{ref('tabla_call_south_processed')}}
  where llamadas_atendidas=1 and respuesta_pregunta_1 in ("1", "2", "3", "4", "5", "6","7")
  group by 1
  order by 1
), numerador as (
  select DATE(fecha_larga) as dia,  count(*) as num FROM {{ref('tabla_call_south_processed')}}
  where llamadas_atendidas=1 and respuesta_pregunta_1 in ("5", "6","7")
  group by 1
  order by 1
)
select a.dia, a.total, b.num,  round(b.num/a.total*100,3) as CSAT from denominador a
left join numerador b using(dia)
order by 1 desc