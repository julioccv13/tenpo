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
    fecha 
    ,timestamp(fecha) as trx_timestamp
    ,'Saldo APP' as nombre
    ,saldo_dia as monto
    ,concat(user,"-",fecha) as trx_id
    ,user
    ,'saldo' as linea
    ,'app' as canal
    ,'n/a' as comercio
    ,CAST(null as STRING) as id_comercio
    ,CAST(null as STRING) as actividad_cd
    ,CAST(null as STRING) as actividad_nac_cd
    ,CAST(null AS FLOAT64) as codact 
    ,null as tipofac 
FROM {{ref('daily_balance')}} trx
where true
    and saldo_dia>0
qualify row_number() over (partition by user,fecha order by fecha)=1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}