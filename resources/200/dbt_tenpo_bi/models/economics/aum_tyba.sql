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
      DATE(FECHA_SALDO) as fecha
      ,FECHA_SALDO as trx_timestamp
      ,'Saldo en Tyba' as nombre
      ,CAST(SUM(SALDO_CUOTAS) AS INT64) as monto
      ,CONCAT('tyba_', user_id,'_',FECHA_SALDO) trx_id 
      ,user_id as user
      ,'aum_tyba' linea
      ,'app' canal
      ,'n/a' as comercio
      , CAST(null as STRING) as id_comercio
      ,CAST(null as STRING) as actividad_cd
      ,CAST(null as STRING) as actividad_nac_cd
      ,CAST(null AS FLOAT64) as codact 
      ,null as tipofac
      ,row_number() over (partition by CONCAT('tyba_', user_id,'_',FECHA_SALDO) order by FECHA_SALDO desc) as row_no_actualizacion 
    FROM {{ref('trx_tyba_saldo')}}
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