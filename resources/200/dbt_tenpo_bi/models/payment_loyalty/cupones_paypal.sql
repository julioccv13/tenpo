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
      cast(pay_transaccion.id as string) purchase_trx_id, -- uuid de la trx que gatillo el cupon
      redeems.id_api_prepaid as reward_trx_id, -- uuid de la prp o trx_id de la economcis (tomar trx id) de cuando recibe el premio
      coupons.id_campaign as campaign_id,
      campaigns.name as campaign,
      campaign_type.transaction_type,
      coupons.name as coupon,
      case when redeems.confirmed and redeems.status = "SUCCEEDED" then true else false end as confirmed,
      campaigns.amount min_coupon_amount,
      campaigns.max_amount max_coupon_amount,
      case 
      when tip_trx = 'RETIRO_APP_PAYPAL' then 'Retiro Paypal' 
      when tip_trx = 'ABONO_APP_PAYPAL' then 'Abono Cuenta USD' 
      end as cupon_merchant,
      pay_transaccion.mto_monto_dolar*valor_dolar_multicaja as cupon_spent,  -- gasto en cupon
      redeems.amount cupon_reward, -- premio abonado por el cupon
FROM {{source('payment_loyalty','redeems')}} redeems
LEFT JOIN {{source('payment_loyalty','coupons')}} coupons on redeems.id_coupon = coupons.id 
LEFT JOIN {{source('payment_loyalty','campaigns')}} campaigns on coupons.id_campaign = campaigns.id 
LEFT JOIN  {{source('payment_loyalty','campaign_type')}} campaign_type on campaigns.campaign_type_id = campaign_type.id
LEFT JOIN {{source('paypal_payments','transactions')}}  paypal on paypal.id = redeems.id_trx
LEFT JOIN {{source('paypal','pay_transaccion')}} pay_transaccion on paypal.transaction_id = pay_transaccion.id
WHERE transaction_type LIKE 'PAYPAL%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY id_user ORDER BY reconciled_date DESC) = 1