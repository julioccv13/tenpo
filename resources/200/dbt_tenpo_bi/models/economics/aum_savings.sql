  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}

{{ 
  config(
    materialized='ephemeral',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    enabled=True
  ) 
}}

WITH 
  target as (
    SELECT DISTINCT
      Fecha_Analisis as fecha
      ,CAST(Fecha_Analisis as TIMESTAMP) trx_timestamp
      ,'Saldo en Bolsillo' as nombre
      ,CAST(SUM(amount) AS INT64) as monto
      ,CONCAT('sb_', user,'_',Fecha_Analisis) trx_id 
      ,user 
      ,'aum_savings' linea
      ,'app' canal
      ,'n/a' as comercio
      , CAST(null as STRING) as id_comercio
      ,CAST(null as STRING) as actividad_cd
      ,CAST(null as STRING) as actividad_nac_cd
      ,CAST(null AS FLOAT64) as codact 
      ,null as tipofac
      ,row_number() over (partition by CONCAT('sb_', user,'_',Fecha_Analisis) order by Fecha_Analisis desc) as row_no_actualizacion 
    FROM {{source('bolsillo','daily_aum')}}
    GROUP BY 1,2,6
    )
    
SELECT DISTINCT
  * EXCEPT(row_no_actualizacion)
FROM target
WHERE TRUE
AND monto > 0
AND row_no_actualizacion = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}

