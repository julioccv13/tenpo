  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}

{{ 
  config(
    materialized='table',
    partition_by = { 'field': 'Fecha_Fin_Analisis_DT', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    enabled=True
  ) 
}}

SELECT  
    * except (email)
FROM {{source('tablones_analisis','tablon_rfmp_v2')}}


{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}