{{ config(tags=["daily", "bi"], materialized='table') }}

SELECT
    DISTINCT *
FROM {{ ref('saldo_app_reconciliation') }}

UNION ALL

SELECT
    DISTINCT *
FROM {{ ref('saldo_app_users') }}

