  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}


{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    enabled=True
  ) 
}}

select 
    dt as fecha,
    ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)] as user,
    * except (deviceinfo),
FROM {{source('clevertap_raw','clevertap_gold_external')}}
WHERE true
{% if is_incremental() %}
    and dt between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}