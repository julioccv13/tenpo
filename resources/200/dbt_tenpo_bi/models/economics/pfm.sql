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

SELECT  
  dt as fecha
  ,parse_timestamp("%Y%m%d%H%M%S", cast(ts as string)) as trx_timestamp
  ,'Home PFM' as nombre
  ,cast(0 as int64) as monto
  ,concat(ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)],"-",ts) as trx_id
  ,ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)] as user
  ,'pfm' as linea
  ,'app' as canal
  ,'n/a' as comercio
  ,CAST(null as STRING) as id_comercio
  ,CAST(null as STRING) as actividad_cd
  ,CAST(null as STRING) as actividad_nac_cd
  ,CAST(null AS FLOAT64) as codact 
  ,null as tipofac
FROM {{source('clevertap_raw','clevertap_gold_external')}}
where eventname = 'Ingresa Sección' and eventprops.product = 'PFM'
QUALIFY
  row_number() over (partition by concat(ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)],"-",ts) order by parse_datetime("%Y%m%d%H%M%S", cast(ts as string)) desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}