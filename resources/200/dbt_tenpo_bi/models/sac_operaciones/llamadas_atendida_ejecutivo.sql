{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='llamadas_atendida_ejecutivo'
    )
}}

select DATE(fecha_larga) as dia, ejecutivo, count(*) as cantidad FROM {{ref('tabla_call_south_processed')}}
where llamadas_atendidas=1
group by 1, 2
order by 1, 3 desc