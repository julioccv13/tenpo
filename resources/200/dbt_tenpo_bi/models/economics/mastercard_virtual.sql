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
    * EXCEPT (es_comercio_presencial)
    ,'mastercard' as linea
FROM {{ ref('mastercard') }}
WHERE (es_comercio_presencial is false OR es_comercio_presencial is null )

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}