{% set partitions_between_to_replace = [
    'date_sub(current_date, interval 15 day)',
    'current_date'
] %}

{{ 
  config(
    tags=["hourly", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
    project=env_var('DBT_PROJECT23', 'tenpo-datalake-sandbox'),
    schema='prepago',
    alias='notifications_history_approved',
    enabled=False

  ) 
}}

WITH data AS 

    (SELECT 
        *
    FROM {{ref('notification_history_fraude')}}
    WHERE estado_trx = 'aprobada'
    AND tipo_tx IN (1,11,55,2)
    )

SELECT
    data.*,
    abs(h.total_currency_value) total_currency_value,
    p.codact as mcc
FROM data 
LEFT JOIN {{ source('prepago', 'prp_movimiento') }} p on p.id_tx_externo = data.id_tx_externo
LEFT JOIN {{ source('transactions_history', 'transactions_history') }} h on h.transaction_id = p.uuid

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}