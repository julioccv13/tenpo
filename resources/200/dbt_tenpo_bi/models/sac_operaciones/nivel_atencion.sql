{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='nivel_atencion'
    )
}}

with atendidas as (
  select DATE(fecha_larga) as dia,  count(*) as cantidad_atendida FROM `tenpo-bi-prod.external.tabla_call_south_processed`
  where llamadas_atendidas=1
  group by 1
  order by 1
), recibidas as (
  select DATE(fecha_larga) as dia,  count(*) as cantidad_recibida FROM `tenpo-bi-prod.external.tabla_call_south_processed`
  where llamadas_recibidas=1
  group by 1
  order by 1
)
select a.dia, a.cantidad_atendida, b.cantidad_recibida, b.cantidad_recibida -  a.cantidad_atendida as cantidad_abandonada,  round(a.cantidad_atendida/b.cantidad_recibida*100,3) as nivel_atencion from atendidas a
left join recibidas b using(dia)
order by 1 desc