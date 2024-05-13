{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='fcr_and_csat_and_tmo'
    )
}}

select a.dia, a.FCR, b.CSAT, c.TMO_min from {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.fcr`' }} a
left join {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.csat`' }} b using(dia)
left join {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.tmo`' }} c using(dia)
