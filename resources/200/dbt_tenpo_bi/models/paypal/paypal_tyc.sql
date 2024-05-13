{% set partitions_to_replace = [
    'current_date',
    'date_sub(date_trunc(current_date, month), INTERVAL 1 day)',
    'date_sub(date_trunc(current_date, month), INTERVAL 2 day)'
] %}


{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'fecha_actualizacion', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}
SELECT 
    *,
    date(updated_at) as fecha_actualizacion
FROM `tenpo-airflow-prod.paypal_payments.paypal_tyc`
WHERE TRUE
{% if is_incremental() %}
    and date(updated_at) in ({{ partitions_to_replace | join(',') }})
{% endif %}
qualify row_number() over (partition by id order by updated_at desc) = 1