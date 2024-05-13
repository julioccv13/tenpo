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
        DATE(created , "America/Santiago") as fecha
        ,md5(identifier) as identificador
        ,created as trx_timestamp
        ,c.name as nombre
        ,amount  as monto
        ,b.id as trx_id
        ,user
        ,'utility_payments' as linea
        ,'app' as canal
        ,u.name as comercio
        ,CAST(u.id as STRING) as id_comercio
        ,CAST(null as STRING) as actividad_cd
        ,CAST(null as STRING) as actividad_nac_cd
        ,CAST(null AS FLOAT64) as codact 
        ,null as tipofac
    FROM {{source('tenpo_utility_payment','bills')}} b
        JOIN  {{source('tenpo_utility_payment','utilities')}} u ON b.utility_id = u.id
        JOIN  {{source('tenpo_utility_payment','categories')}} c ON c.id = u.category_id 
    WHERE 
        b.status = 'SUCCEEDED'
    QUALIFY
        row_number() over (partition by CAST(b.id AS STRING) order by updated desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
