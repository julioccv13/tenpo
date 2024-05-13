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

SELECT
  a.id,
  date(a.created_at,"America/Santiago") fecha,
  a.created_at,
  a.updated_at,
  a.amount,
  c.id as user,
  a.recipient_type,
  a.reference_id,
  a.sender_holder_id,
  a.sender_institution_id,
  a.sender_number,
  a.sender_type,
  a.status,
  a.transaction_date,
  a.widget_token,
  a.payment_intent_date,
  a.product_type
FROM {{source('tenpo_fintoc','intent')}} a
JOIN {{source('identity','ruts')}} b on recipient_holder_id = trim(concat(rut,dv))
JOIN {{ ref('users_tenpo') }} c using (tributary_identifier)

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}