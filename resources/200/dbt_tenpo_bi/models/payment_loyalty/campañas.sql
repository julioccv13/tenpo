{{ 
  config(
    tags=["daily", "bi"], 
    materialized='table',
    alias='campaigns'
  )
}}

SELECT 
    b.id as campaign_id,
    b.name campaign,
    a.name as campaign_type,
    a.transaction_type,
    a.new_user,
    c.name as cupon,
    c.creation_date,
    b.campaign_start,
    b.campaign_end,
    c.quantity,
    c.redeemed,
    b.amount as min_amount, -- monto minimo
    b.max_amount
FROM {{source('payment_loyalty','campaign_type')}} a
JOIN {{source('payment_loyalty','campaigns')}} b on a.id = b.campaign_type_id
JOIN {{source('payment_loyalty','coupons')}} c on b.id = c.id_campaign
