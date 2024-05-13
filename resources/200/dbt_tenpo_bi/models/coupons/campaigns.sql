{{ config(tags=["hourly", "bi"], materialized='table') }}

SELECT 
distinct
ca.id as id
,ca.name as name
,ca.state
,ca.campaign_start
,ca.campaign_end
,ct.id as campaign_type_id
,ct.name as campaign_type_name
,CASE 
    WHEN ct.name in ('GENERICA_FECHA_OB_CON_CASHIN') THEN 'Cashin'
    WHEN ct.name in ('GENERICA_MERCHANT_RETURN') THEN 'Cashback por comercio'
    WHEN ct.name in ('FIRST_TRX_PURCHASE_NEW_USERS', 'FIRST_TRX_PURCHASE_OLD_USERS') THEN 'Cashback compra'
    WHEN ct.name in ('FIRST_TRX_TOP_UP_NEW_USERS', 'FIRST_TRX_TOP_UP_OLD_USERS') THEN 'Cashback recarga'
    WHEN ct.name in ('FIRST_TRX_PAY_BILL_NEW_USERS', 'FIRST_TRX_PAY_BILL_OLD_USERS') THEN 'Cashback pago de cuentas'
    END AS campaign_type_characterization
,CASE 
    WHEN ct.name in ('GENERICA_FECHA_OB_CON_CASHIN', 'FIRST_TRX_PURCHASE_NEW_USERS', 'FIRST_TRX_PURCHASE_OLD_USERS', 'FIRST_TRX_TOP_UP_NEW_USERS', 'FIRST_TRX_PAY_BILL_NEW_USERS') THEN 'Adquisición'
    WHEN ct.name in ('GENERICA_MERCHANT_RETURN', 'FIRST_TRX_TOP_UP_OLD_USERS', 'FIRST_TRX_PAY_BILL_OLD_USERS') THEN 'Fidelización'
    END AS campaign_objective_characterization
,ca.amount
,ca.max_amount
FROM {{source('payment_loyalty','campaigns')}} ca 
JOIN {{source('payment_loyalty','campaign_type')}}  ct ON ct.id = ca.campaign_type_id 