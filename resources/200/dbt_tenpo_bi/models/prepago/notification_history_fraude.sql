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
    project=env_var('DBT_PROJECT23', 'tenpo-datalake-sandbox'),
    schema='prepago',
    alias='notifications_history_fraude'

  ) 
}}

WITH datos as

(SELECT
    notifications.id,
    TIMESTAMP_SECONDS(CAST(CAST(notifications.created_at as INT64) as INT64)) fecha_creacion,
    DATE(TIMESTAMP_SECONDS(CAST(CAST(notifications.created_at as INT64) as INT64))) fecha,
    TIME( TIMESTAMP_SECONDS(CAST(CAST(notifications.created_at as INT64) as INT64))) hora_creacion,
    TIMESTAMP_SECONDS(CAST(CAST(notifications.updated_at as INT64) as INT64)) fecha_actualizacion,
    usuario.uuid as user,
    users.rut,
    body.header.cuenta as contrato,
    body.header.pan,
    SUBSTRING(trim(SAFE_CONVERT_BYTES_TO_STRING(body.base64_data)),297,6) id_tx_externo,
    body.body.tipo_tx tipo_trx,
    o.descripcion_sia as description_tipo_trx,
    case when body.body.resolucion_tx in (1,400) then 'aprobada' else 'rechazada'end as estado_trx,
    r.descripcion_sia as description_estado_trx,
    cast(body.body.sd_value as float64) saldo_disponible,
    cast(body.body.il_value  as float64) monto_trx,
    case 
        when length(body.body.merchant_code) = 1 then concat("00000000000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 2 then concat("0000000000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 3 then concat("000000000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 4 then concat("00000000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 5 then concat("0000000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 6 then concat("000000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 7 then concat("00000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 8 then concat("0000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 9 then concat("000000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 10 then concat("00000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 11 then concat("0000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 12 then concat("000",body.body.merchant_code) 
        when length(body.body.merchant_code) = 13 then concat("00",body.body.merchant_code) 
        when length(body.body.merchant_code) = 14 then concat("0",body.body.merchant_code) 
      else body.body.merchant_code
      end as merchant_code,
    body.body.merchant_name,
    body.body.country_description as country,
    body.body.place_name as place,
    case 
      when SUBSTRING(trim(SAFE_CONVERT_BYTES_TO_STRING(body.base64_data)),197,1) = '0' then 'false' 
      when SUBSTRING(trim(SAFE_CONVERT_BYTES_TO_STRING(body.base64_data)),197,1) = '1' then 'true'
    else 'N/A' end as es_comercio_presencial,
    body.body.country_iso_3266_code country_iso_code,
    CASE
        WHEN body.body.country_iso_3266_code = 152 THEN 'nacional'
        ELSE 'internacional'
        END AS origen_trx,
    SUBSTRING(trim(SAFE_CONVERT_BYTES_TO_STRING(body.base64_data)),145,6) adquirente,
    trim(SAFE_CONVERT_BYTES_TO_STRING(body.base64_data)) as decode_b64,
FROM {{source('notifications','notifications_history_v2')}} notifications
LEFT JOIN {{source('prepago','prp_cuenta')}} cuenta ON CAST(notifications.body.header.cuenta as INT64) = CAST(RIGHT(CAST(cuenta.cuenta as STRING), 7) as INT64)
LEFT JOIN {{source('prepago','prp_usuario')}} usuario ON cuenta.id_usuario = usuario.id
LEFT JOIN {{ source('tenpo_users', 'users') }} users on usuario.uuid = users.id
LEFT JOIN {{ source('seed_data', 'operaciones_sia') }} o on notifications.body.body.tipo_tx = o.codigo_trx_id
LEFT JOIN {{ source('aux', 'resoluciones_sia') }} r on notifications.body.body.resolucion_tx = r.resolucion_sia

)

SELECT
     distinct
      datos.*
FROM datos
WHERE TRUE
{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}