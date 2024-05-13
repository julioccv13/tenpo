{{ config(materialized='ephemeral') }}

with prp_transacciones as (
    SELECT
        DATE(m.fecha_creacion  , "America/Santiago") AS fecha,
        m.fecha_creacion,
        m.fecha_actualizacion,
        tipofac,
        m.id,
        m.impfac as monto,
        m.uuid as trx_id,
        u.uuid as tenpo_uuid,
        nomcomred as comercio,
        m.estado,
        m.indnorcor,
        row_number() over (partition by CAST(m.uuid AS STRING) order by m.fecha_actualizacion desc) as row_no_actualizacion 
    FROM {{ source('prepago', 'prp_cuenta') }} c
        JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
        JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
        JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
)

SELECT
* EXCEPT(row_no_actualizacion)
FROM prp_transacciones
where row_no_actualizacion = 1