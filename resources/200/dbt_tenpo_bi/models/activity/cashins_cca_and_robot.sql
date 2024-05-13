{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "user"
  ) 
}}


WITH 
    cashins as (

        SELECT
            DISTINCT
            trx.id id,
            trx.userId as user,
            trx.amount,
            trx.created,
            bank.name as bank,
            'cca' source_cashin
        FROM {{source('payment_cca','payment_transaction')}} trx
        JOIN {{source('payment_cca','bank')}} bank on replace(trx.originSbifCode, '0', '') = CAST(bank.sbif_code AS STRING)
        WHERE 
          status = 'AUTHORIZED'

        UNION ALL 
        (
        SELECT 
            distinct 
            cashin.id,
            cashin.user_id user,
            cashin.amount,
            cashin.created,
            bank.name as bank,
            'robot' source_cashin
        FROM {{source('payment_cca','payment_request')}} cashin
        JOIN {{source('payment_cca','bank_account')}} account on cashin.bank_account_id = account.id
        JOIN {{source('payment_cca','bank')}} bank on bank.id = account.bank_id
        WHERE 
            status = 'AUTHORIZED'
        )

)

SELECT 
    *
FROM cashins
