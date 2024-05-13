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

SELECT DISTINCT
  DATE(co.created_date  , "America/Santiago") fecha
  ,co.created_date as trx_timestamp
  ,'Rescate' nombre
  ,amount  as monto
  ,co.cash_out_id  trx_id
  ,user_id user
  ,'cash_out_savings' linea
  ,'app' canal
  ,'n/a' as comercio
  ,CAST(null as STRING) as id_comercio
  ,CAST(null as STRING) as actividad_cd
  ,CAST(null as STRING) as actividad_nac_cd
  ,CAST(null AS FLOAT64) as codact 
  ,null as tipofac
FROM {{ source('payment_savings', 'cash_out') }} co
 JOIN {{ ref('users_savings') }} u  ON user_id = id 
WHERE 
  status in ( 'SUCCESSFUL', 'PENDING', 'CONFIRMED', 'CREATED', 'PENDING_SETTLEMENT') 
  AND register_status in ('FINISHED')
  AND onboarding_status in ('FINISHED')
  AND origin = 'SAVINGS'
QUALIFY 
  row_number() over (partition by CAST(cash_out_id AS string) order by co.confirmed_date desc)  = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}