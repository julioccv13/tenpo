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
      transaction.id purchase_trx_id, -- uuid de la trx que gatillo el cupon
      redeems.id_api_prepaid as reward_trx_id, -- uuid de la prp o trx_id de la economcis (tomar trx id) de cuando recibe el premio
      coupons.id_campaign as campaign_id,
      campaigns.name as campaign,
      campaign_type.transaction_type,
      coupons.name as coupon,
      case when redeems.confirmed and redeems.status = "SUCCEEDED" then true else false end as confirmed,
      campaigns.amount min_coupon_amount,
      campaigns.max_amount max_coupon_amount,
      operator.name cupon_merchant, -- comercio que gatila el cupon
      topup.amount cupon_spent, -- gasto en cupon
      redeems.amount cupon_reward, -- premio abonado por el cupon
FROM {{source('payment_loyalty','redeems')}} redeems
LEFT JOIN {{source('payment_loyalty','coupons')}} coupons on redeems.id_coupon = coupons.id 
LEFT JOIN {{source('payment_loyalty','campaigns')}} campaigns on coupons.id_campaign = campaigns.id 
LEFT JOIN  {{source('payment_loyalty','campaign_type')}} campaign_type on campaigns.campaign_type_id = campaign_type.id
LEFT JOIN {{source('tenpo_topup','topup')}} topup on topup.id = redeems.id_trx
LEFT JOIN {{source('tenpo_topup','transaction')}}  transaction on transaction.id = topup.transaction_id
LEFT JOIN {{source('tenpo_topup','product_operator')}} pr on topup.product_operator_id = pr.id
LEFT JOIN {{source('tenpo_topup','operator')}} operator on operator.id = pr.operator_id
WHERE transaction_type LIKE 'TOP_UP%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY reconciled_date DESC) = 1