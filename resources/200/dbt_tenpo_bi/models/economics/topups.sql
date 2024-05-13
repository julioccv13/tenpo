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
    DATE(r.created_at  , "America/Santiago") as fecha 
    ,md5(r.identifier) as suscriptor
    ,r.created_at as trx_timestamp
    ,pr.name as nombre
    ,amount as monto
    ,t.id as trx_id
    ,t.user_id as user
    ,'top_ups' as linea
    ,'app' as canal
    ,o.name as comercio
    ,CAST(o.id as STRING) id_comercio
    ,CAST(null as STRING) as actividad_cd
    ,CAST(null as STRING) as actividad_nac_cd
    ,CAST(null AS FLOAT64) as codact 
    ,null as tipofac
FROM {{source('tenpo_topup','topup')}}  r
      JOIN {{source('tenpo_topup','product_operator')}} p ON r.product_operator_id = p.id
      JOIN {{source('tenpo_topup','operator')}}  o ON o.id= p.operator_id
      JOIN {{source('tenpo_topup','transaction')}} t ON r.transaction_id = t.id
      JOIN {{source('tenpo_topup','product')}} pr ON p.product_id = pr.id
WHERE
    r.status  = 'SUCCEEDED'
    AND t.status ='SUCCEEDED'
QUALIFY
    row_number() over (partition by CAST( t.id AS STRING) order by r.updated_at desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}