{{ config(tags=["daily", "bi"], materialized='ephemeral') }}

SELECT 
    DISTINCT
    A.FECHA_CARGA AS fecha,
    D.id as user,
    SaldoNoConciliado	+ SaldoConciliado as saldo_app
FROM {{ source('reconciliation', 'trx_mpj_saldo') }} A
JOIN  {{ ref('contratos') }} B USING (contrato)
JOIN {{ source('identity', 'ruts') }} C ON C.rut_complete = B.numero_documento
JOIN {{ source('tenpo_users', 'users') }} D ON D.rut = C.rut_complete
