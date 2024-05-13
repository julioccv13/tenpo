{{ config(tags=["hourly", "bi"], materialized='table') }}


SELECT
    userid as user
    ,CASE WHEN type ='CASHIN_TEF' THEN 'cashin' WHEN type = 'CASHOUT_TEF' THEN 'cashout' END AS tipo_trx
    ,'tef_cca' AS canal

    --,CAST(trx.id AS STRING) AS id_trx
    ,CAST( COALESCE(prp.uuid, trx.id) AS STRING) as id_trx
    ,CAST(amount AS NUMERIC) AS monto_trx
    ,trx.created AS ts_trx

    -- ,origin_bank.name AS cico_banco_tienda_origen
    -- ,destination_bank.name AS cico_banco_tienda_destino
    ,COALESCE(origin_bank.name, '___TENPO_APP___') AS cico_banco_tienda_origen
    ,COALESCE(destination_bank.name, '___TENPO_APP___') AS cico_banco_tienda_destino

    
    ,case when originaccounttype = 'd9f51d27-c4a4-42b9-994b-5bd38a38d360' then 'Tenpo Prepago'
            else null end as originaccounttype
    ,case when destinationaccounttype = 'd0c93549-8aa6-48eb-890f-75d93f38f37e' then 'Cuenta Corriente'
            when destinationaccounttype = 'efcc47a5-bb40-4201-9e8d-b69318d1d46c' then 'Cuenta Vista'
            when destinationaccounttype = '7d20315f-3f5d-4939-8bce-fb91275f9e11' then 'Cuenta Ahorro'
            when destinationaccounttype = 'd9f51d27-c4a4-42b9-994b-5bd38a38d360' then 'Tenpo Prepago'
            else null end as destinationaccounttype

    ,originaccountnumber as cuenta_origen
    ,destinationaccountnumber as cuenta_destino

    --,originaccountrun as rut_origen
    --,destinationaccountrun as rut_destino

    ,originaccountrun = destinationaccountrun AS trx_mismo_rut
    ,CASE WHEN u.id IS NULL THEN false else true end as tiene_cuenta_tenpo
    ,TRX.email
    --,SUBSTR(destinationaccountrun, 4) rut
    --,CONCAT(SUBSTR(SUBSTR(destinationaccountrun, 4),1,LENGTH(SUBSTR(destinationaccountrun, 4))-1) ,'-',RIGHT(SUBSTR(destinationaccountrun, 4),1)) rut_format

FROM {{ source('payment_cca', 'payment_transaction') }} TRX
    LEFT JOIN {{ source('payment_cca', 'bank') }} origin_bank ON CAST(trx.originSbifCode AS INT64) = origin_bank.sbif_code
    LEFT JOIN {{ source('payment_cca', 'bank') }} destination_bank  ON CAST(trx.destinationSbifCode AS INT64) = destination_bank.sbif_code
    LEFT JOIN {{ source('prepago', 'prp_movimiento') }} prp ON prp.id_tx_externo = TRX.id
    LEFT JOIN {{ source('tenpo_users', 'users') }} u on u.rut = CONCAT(SUBSTR(SUBSTR(destinationaccountrun, 4),1,LENGTH(SUBSTR(destinationaccountrun, 4))-1) ,'-',RIGHT(SUBSTR(destinationaccountrun, 4),1))
WHERE 
    STATUS = 'AUTHORIZED' 
QUALIFY ROW_NUMBER() OVER (PARTITION BY trx.id ORDER BY trx.updated DESC) = 1