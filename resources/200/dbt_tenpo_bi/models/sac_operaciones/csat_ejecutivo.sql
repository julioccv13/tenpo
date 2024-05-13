{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='csat_ejecutivo'
    )
}}

with denominador as (
  select DATE(fecha_larga) as dia, ejecutivo,  count(*) as total FROM {{ref('tabla_call_south_processed')}}
  where llamadas_atendidas=1 and respuesta_pregunta_1 in ("1", "2", "3", "4", "5", "6","7")
  group by 1, 2
  order by 1
), numerador as (
  select DATE(fecha_larga) as dia, ejecutivo, count(*) as num FROM {{ref('tabla_call_south_processed')}}
  where llamadas_atendidas=1 and respuesta_pregunta_1 in ("5", "6","7")
  group by 1, 2
  order by 1
)
select a.dia, a.ejecutivo, a.total, b.num,  round(b.num/a.total*100,3) as CSAT from denominador a
left join numerador b on a.dia=b.dia and a.ejecutivo=b.ejecutivo
order by 1 desc