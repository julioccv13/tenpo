{{ config(materialized='ephemeral') }}


SELECT
    DATE(m.fecha_creacion  , "America/Santiago") AS fecha,
    m.fecha_creacion as trx_timestamp,
    CASE 
        WHEN tipofac = 3006 THEN 'Suscrip. dist. peso'
        WHEN tipofac = 3007 THEN 'Compra dist. peso'
        WHEN tipofac = 3010 THEN 'Devolución compra'
        WHEN tipofac = 3011 THEN 'Devolución compra com. relacionado'
        WHEN tipofac = 3012 THEN 'Devolución compra nacional'
        WHEN tipofac = 3028 THEN 'Compra peso'
        WHEN tipofac = 3029 THEN 'Suscrip. peso'
        WHEN tipofac = 3030 THEN 'Devolución compra peso'
        WHEN tipofac in ( 3009, 3031, 5) THEN 'Nacional'
        END AS nombre,
    CASE 
        WHEN tipofac in (3011,3012,3030) THEN m.impfac*-1 
        WHEN tipofac = 3010 THEN m.impfac *-1.024
        WHEN tipofac in (3006, 3007) THEN m.impfac *1.024
        ELSE m.impfac END as monto,
    m.uuid as trx_id,
    u.uuid as user,
    'app' as canal,
    nomcomred as comercio,
    CAST(m.codcom as STRING) as id_comercio,
    actividad_cd,
    actividad_nac_cd,
    m.codact,
    tipofac,
    es_comercio_presencial,
FROM {{ source('prepago', 'prp_cuenta') }} c
    JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
    JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
    JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
WHERE 
    tipofac in (3006,3007,3028,3029,3009, 3031, 5, 3010,3011,3012,3030) 
    AND (m.estado  in ('PROCESS_OK','AUTHORIZED') 
    OR (m.estado = 'NOTIFIED' AND DATE(m.fecha_creacion  , "America/Santiago") BETWEEN DATE_SUB(CURRENT_DATE("America/Santiago"), INTERVAL 3 DAY) AND CURRENT_DATE("America/Santiago")))
    AND indnorcor  = 0
QUALIFY 
    row_number() over (partition by CAST(m.uuid AS STRING) order by m.fecha_actualizacion desc) = 1