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
  fecha, 
  nombre, 
  monto, 
  trx_id, 
  user, 
  linea, 
  last_ndd_service_bm,
  canal, comercio
FROM(
  SELECT
    DATE(m.fecha_creacion  , "America/Santiago") AS fecha,
    'Cobro p2p' as nombre,
    m.impfac  as monto,
    m.uuid as trx_id,
    u.uuid as user,
    'p2p_cobro' linea,
    'app' canal,
    'n/a' as comercio,
    CAST(null as STRING) as id_comercio,
    actividad_cd,
    actividad_nac_cd,
    m.codact ,
    tipofac,
    FROM {{ source('prepago', 'prp_cuenta') }} c
        JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
        JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
        JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
WHERE tipofac in (336)
    AND m.estado  in ('PROCESS_OK')
    AND m.estado_de_negocio  in ('CONFIRMED', 'OK')
    AND id_tx_externo not like 'MC_%'
    AND indnorcor  = 0)
LEFT JOIN (
        select distinct 
        id as user, 
        last_ndd_service_bm 
        FROM {{ ref('users_allservices') }}
        where id is not null
        ) USING (user)

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}