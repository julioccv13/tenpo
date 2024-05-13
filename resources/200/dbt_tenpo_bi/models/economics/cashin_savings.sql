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
      DATE(ci.created_date  , "America/Santiago") AS fecha
      ,ci.created_date as trx_timestamp
      ,IF(re.id is null,'Aporte Manual','Aporte Regla') nombre
      ,ci.amount  as monto
      ,ci.cash_in_id as trx_id
      ,u.id as user
      ,'cash_in_savings' linea
      ,'app' canal
      ,'n/a' as comercio
      ,CAST(null as STRING) as id_comercio
      ,CAST(null as STRING) as actividad_cd
      ,CAST(null as STRING) as actividad_nac_cd
      ,CAST(null AS FLOAT64) as codact
      ,null as tipofac
    FROM {{ source('payment_savings', 'cash_in') }} ci
      JOIN {{ ref('users_savings') }} u  ON ci.user_id = u.id  --
      LEFT JOIN {{source('savings_rules','rule_execution_history')}} re USING (cash_in_id)
    WHERE 
      ci.status in ( 'SUCCESSFUL', 'PENDING', 'CONFIRMED', 'CREATED')
      AND u.register_status in ('FINISHED')
      AND u.onboarding_status in ('FINISHED')
      AND u.origin = 'SAVINGS'
    QUALIFY 
      row_number() over (partition by CAST(cash_in_id AS string) order by ci.confirmed_date desc)  = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}