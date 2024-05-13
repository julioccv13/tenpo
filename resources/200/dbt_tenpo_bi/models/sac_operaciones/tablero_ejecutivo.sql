{{ 
    config(
        materialized='table'
        ,tags=["hourly"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='control_tower'
        ,alias='tablero_ejecutivo'
    )
}}

SELECT
a.dia, a.ejecutivo, a.cantidad as llamadas_atendidas, b.total as denominador_csat, b.num as numerador_csat,b.CSAT,
c.total as denominador_fcr, c.num as numerador_fcr, c.FCR, d.TMO_min
FROM {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.llamadas_atendida_ejecutivo`' }} a
LEFT JOIN {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.csat_ejecutivo`' }} b on a.dia=b.dia and a.ejecutivo=b.ejecutivo
LEFT JOIN {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.fcr_ejecutivo`' }} c on a.dia=c.dia and a.ejecutivo=c.ejecutivo
LEFT JOIN {{ '`'+env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')+'.control_tower.tmo`' }} d on a.dia=d.dia and a.ejecutivo=d.ejecutivo