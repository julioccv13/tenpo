{% set partitions_between_to_replace = [
    'date_sub(current_date, interval 5 day)',
    'current_date'
] %}

{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite'
  ) 
}}


SELECT *, date(updated_at,"America/Santiago") fecha,  FROM `tenpo-airflow-prod.crossborder.crossborder_tyc`

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
