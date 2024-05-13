{{ config(tags=["daily", "bi"], materialized='ephemeral') }}


SELECT 
    DISTINCT
    A.fecha,
    B.id as user,
    A.SaldoAPP as saldo_app
FROM {{ source('tenpo_users', 'saldos') }}  A
JOIN {{ source('tenpo_users', 'users') }} B ON A.rut_hash = B.tributary_identifier