{{ config(materialized='table',  tags=["hourly", "bi"]) }}

SELECT 
  id,
  md5(mail) as mail, 
  md5(mail_cuenta) as mail_cuenta,
  saldo,
  filename,
  date
FROM {{source('aux_paypal','aux_saldos')}}