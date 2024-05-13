{{ 
  config(
    tags=["hourly", "bi"], 
    materialized='ephemeral',
    partition_by = { 'field': 'fecha', 'data_type': 'date' }
  )
}}

SELECT 
    DISTINCT
      date(redeems.created_at) fecha,
      redeems.created_at as enter_coupon_date, -- fecha de ingreso del cupon en la app
      redeems.redeem_date redeem_coupon_date, -- fecha que quemo u utilizo el cupon
      redeems.id_user as user,
      redeems.id_trx purchase_trx_id, -- uuid de la trx que gatillo el cupon
      redeems.id_api_prepaid as reward_trx_id, -- uuid de la prp o trx_id de la economcis (tomar trx id) de cuando recibe el premio
      coupons.id_campaign as campaign_id,
      campaigns.name as campaign,
      campaign_type.transaction_type,
      coupons.name as coupon,
      case when redeems.confirmed and redeems.status = "SUCCEEDED" then true else false end as confirmed,
      campaigns.amount min_coupon_amount,
      campaigns.max_amount max_coupon_amount,
      case 
                when destination_currency = "ar_ars" then "Argentina"
                when destination_currency = "uy_uyu" then "Uruguay"
                when destination_currency = "py_pyg" then "Paraguay"
                when destination_currency = "us_usd" then "EE.UU"
                when destination_currency = "mx_mxn" then "México"
                when destination_currency = "br_brl" then "Brasil"
                when destination_currency = "co_cop" then "Colombia"
                when destination_currency = "pe_pen" then "Perú"
                when destination_currency = "bo_bob" then "Bolivia"
                else null
            end as cupon_merchant,
      crossborder.origin_amount cupon_spent, -- gasto en cupon
      redeems.amount cupon_reward, -- premio abonado por el cupon
FROM {{source('payment_loyalty','redeems')}} redeems
LEFT JOIN {{source('payment_loyalty','coupons')}} coupons on redeems.id_coupon = coupons.id 
LEFT JOIN {{source('payment_loyalty','campaigns')}} campaigns on coupons.id_campaign = campaigns.id 
LEFT JOIN  {{source('payment_loyalty','campaign_type')}} campaign_type on campaigns.campaign_type_id = campaign_type.id
LEFT JOIN {{source('crossborder','transaction')}} crossborder on crossborder.id = redeems.id_trx
WHERE transaction_type LIKE 'CROSSBORDER%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY reconciled_date DESC) = 1