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

SELECT
    DATE(m.fecha_creacion  , "America/Santiago") AS fecha
    ,m.fecha_actualizacion as trx_timestamp
    ,'Pago enviado p2p' as nombre
    ,m.impfac  as monto
    ,m.uuid as trx_id
    ,u.uuid as user
    ,'p2p' linea
    ,'app' canal
    ,'n/a' as comercio
    ,CAST(null as STRING) as id_comercio
    ,actividad_cd
    ,actividad_nac_cd
    ,m.codact 
    ,tipofac
FROM {{ source('prepago', 'prp_cuenta') }} c
    JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
    JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
    JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
WHERE tipofac in (335)
    AND m.estado  in ('PROCESS_OK')
    AND m.estado_de_negocio  in ('CONFIRMED', 'OK')
    AND id_tx_externo not like 'MC_%'
    AND indnorcor  = 0
QUALIFY 
    row_number() over (partition by CAST(m.uuid AS STRING) order by m.fecha_actualizacion desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}