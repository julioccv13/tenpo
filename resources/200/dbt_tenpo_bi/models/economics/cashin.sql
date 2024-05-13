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
    DATE(m.fecha_creacion  , "America/Santiago") AS fecha,
    m.fecha_creacion as trx_timestamp,
    CASE WHEN tipofac = 3001 THEN 'Cashin tef' 
            WHEN tipofac = 3002 THEN 'Cashin físico Klap' 
            WHEN tipofac = 3016 THEN 'Cashin moneysend' 
            WHEN tipofac = 711 THEN 'Cashin físico Unired' 
            WHEN tipofac = 90 THEN 'Cashin físico Sencillito' 
            WHEN tipofac = 3053 THEN 'Cashin Pago Nomina'
            END AS nombre,
    impfac  as monto,
    m.uuid as trx_id,
    u.uuid as user,
    'cash_in' linea,
    'app' canal,
    'n/a' as comercio,
    CAST(m.codcom as STRING) as id_comercio,
    actividad_cd,
    actividad_nac_cd,
    m.codact,
    m.tipofac
FROM {{ source('prepago', 'prp_cuenta') }} c
    JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
    JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
    JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
WHERE 
    m.estado    = 'PROCESS_OK'
    AND indnorcor  = 0
    AND tipofac in (3001,3002,3016,711,90,3053)
    AND estado_de_negocio in ('OK','CONFIRMED')
QUALIFY 
    row_number() over (partition by CAST(m.uuid AS string) order by m.fecha_actualizacion desc) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}

