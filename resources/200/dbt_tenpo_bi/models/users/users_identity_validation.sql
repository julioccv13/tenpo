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
  )
}}

with pre_data as

    (select 
        b.user,
        a.id,
        a.check_id,
        date(a.created) fecha,
        a.created,
        a.result,
        b.type,
        json_query(a.result, '$.reports') as report_raw,
    from {{ source('tenpo_users', 'identity_validation_results') }} a
    join {{ source('tenpo_users', 'identity_validations') }} b on b.id = cast(a.check_id as string)
    ), 

    arr_data as 

    (select 
        *,
        LEFT(SUBSTR(report_raw, (STRPOS(report_raw, '[') + 1), (LENGTH(report_raw) - STRPOS(report_raw, '['))) , LENGTH(report_raw) - 2 ) AS arr_result
    from pre_data
    )

select 
      id,
      check_id,
      fecha,
      created,
      user,
      report_raw,
      type,
      json_query(arr_result, '$.result') as resultado_final,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.data_comparison'),'$.result') result_data_comparison,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.data_consistency'),'$.result') result_data_consistency,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.data_validation'),'$.result') result_data_validation,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.compromised_document'),'$.result') result_compromised_document,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.age_validation'),'$.result') result_age_validation,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.image_integrity'),'$.result') result_image_integrity,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.police_record'),'$.result') result_police_record,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.visual_authenticity'),'$.result') result_visual_authenticity,
      json_query(json_query(json_query(arr_result, '$.breakdown'),'$.face_comparison'),'$.result') result_face_comparison,
from arr_data
where true
{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}