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

  SELECT DISTINCT
    DATE(updated_at  , "America/Santiago") AS fecha
    ,updated_at as trx_timestamp
    ,tipo_operacion nombre
    ,monto
    ,id as trx_id
    ,user_id as user
    ,concat(tipo_operacion,'_tyba') linea
    ,'app' canal
    ,'n/a' as comercio
    ,CAST(null as STRING) as id_comercio
    ,CAST(null as STRING) as actividad_cd
    ,CAST(null as STRING) as actividad_nac_cd
    ,CAST(null AS FLOAT64) as codact
    ,null as tipofac
  FROM  {{ ref('trx_tyba') }}
  where status = 'finished'

{% if is_incremental() %}
    and dt between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}