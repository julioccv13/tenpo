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
    user,
    fecha,
    trx_timestamp,
    monto,
    trx_id
FROM {{ ref('cashin') }}
QUALIFY 
    row_number() over (partition by user order by trx_timestamp asc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}