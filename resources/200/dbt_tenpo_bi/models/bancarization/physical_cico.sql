{{ 
  config(
    materialized='ephemeral', 
  ) 
}}

SELECT
    tenpo_uuid
    ,'cashin' AS tipo_trx
    ,'fisico' AS canal

    ,CAST(m.id AS STRING) AS id_trx
    ,CAST(monto AS NUMERIC) AS monto_trx
    ,m.fecha_creacion AS ts_trx

    ,CAST(null AS STRING) AS cico_banco_tienda_origen
    ,CAST(null AS STRING) AS cico_banco_tienda_destino

    ,CAST(null AS STRING) as originaccounttype
    ,CAST(null AS STRING) as destinationaccounttype

    ,CAST(null AS STRING) as cuenta_origen
    ,CAST(null AS STRING) as cuenta_destino

    ,CAST(null AS STRING) as rut_origen
    ,CAST(null AS STRING) as rut_destino

    ,true AS trx_mismo_rut

    FROM {{ ref('prp_movimiento') }} m
WHERE m.estado = 'PROCESS_OK'
    AND indnorcor  = 0
    AND tipofac in (3002)
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(m.id AS INT64) ORDER BY m.fecha_actualizacion DESC) = 1

UNION ALL

SELECT
    tenpo_uuid
    ,'cashout' AS tipo_movimiento
    ,'fisico' AS canal

    ,'cashout_fis_' ||  CAST(m.id AS STRING) AS id_trx
    ,CAST(monto AS NUMERIC) AS monto_trx
    ,m.fecha_creacion AS ts_trx
    
    ,CAST(null AS STRING) AS cico_banco_tienda_origen
    ,CAST(null AS STRING) AS cico_banco_tienda_destino

    ,CAST(null AS STRING) as originaccounttype
    ,CAST(null AS STRING) as destinationaccounttype

    ,CAST(null AS STRING) as cuenta_origen
    ,CAST(null AS STRING) as cuenta_destino

    ,CAST(null AS STRING) as rut_origen
    ,CAST(null AS STRING) as rut_destino

    ,true AS trx_mismo_rut

    FROM {{ ref('prp_movimiento') }} m
WHERE 
    (
        (m.estado  = 'PROCESS_OK' AND tipofac =3004) )
    AND indnorcor  = 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(m.id AS INT64) ORDER BY m.fecha_actualizacion DESC) = 1
