{{ config(tags=["hourly", "bi"], enabled=True, materialized='table') }}
WITH   
  economics AS(
    SELECT
      *
    FROM {{ ref('economics') }}
    ),
   cupones as(
    SELECT DISTINCT
      r.id_user id_usuario,
      ca.name as campaign_name,
      cp.name coupon_name,
      r.confirmed status_cupon,
      CASE 
        WHEN t.name in ('GENERICA_FECHA_OB_CON_CASHIN') THEN 'Cashin'
        WHEN t.name in ('GENERICA_MERCHANT_RETURN', 'CASHBACK_PURCHASE') THEN 'Cashback por comercio'
        WHEN t.name in ('FIRST_TRX_PURCHASE_NEW_USERS', 'FIRST_TRX_PURCHASE_OLD_USERS') THEN 'Cashback compra'
        WHEN t.name in ('FIRST_TRX_TOP_UP_NEW_USERS', 'FIRST_TRX_TOP_UP_OLD_USERS', 'CASHBACK_TOP_UP') THEN 'Cashback recarga'
        WHEN t.name in ('FIRST_TRX_PAY_BILL_NEW_USERS', 'FIRST_TRX_PAY_BILL_OLD_USERS', 'CASHBACK_PAY_BILL') THEN 'Cashback pago de cuentas'
        END AS tipo_cupon,
      r.amount as coup_amount,
      DATETIME(r.redeem_date , "America/Santiago") as redeem_timestamp,
    FROM {{source('payment_loyalty','campaigns')}} ca
    LEFT JOIN {{source('payment_loyalty','coupons')}} cp ON ca.id=cp.id_campaign 
    LEFT JOIN {{source('payment_loyalty','redeems')}} r ON cp.id = r.id_coupon
    JOIN {{source('payment_loyalty','campaign_type')}} t ON ca.campaign_type_id =  t.id
    ) ,
      
  onboardings as (
    SELECT DISTINCT 
      id id_usuario, 
      DATE(ob_completed_at , "America/Santiago") fecha_ob,
      IF(pep.id is not null, true, false) is_pep,
      IF(source like '%IG%', true, false) source_iyg
    FROM {{ ref('users_tenpo') }}  
    LEFT JOIN {{source('pep','pep_user_id')}} pep USING(id)
    WHERE 
      state in (4,7,8,21,22) 
    ),
  first_cashins as ( 
    SELECT DISTINCT 
      user id_usuario,
      fecha_fci
    FROM {{ ref('funnel_fci') }} --`tenpo-bi.funnel.cp_funnel_fci` 
    ),
  activaciones as (
    SELECT DISTINCT 
      user id_usuario,
      fecha_activacion
    FROM {{ ref('funnel_activacion') }} --`tenpo-bi.funnel.cp_usuarios_tarjeta`
    )

   SELECT DISTINCT 
    id_usuario user,
    onboardings.fecha_ob ,
    first_cashins.fecha_fci ,
    fecha_activacion fecha_activacion ,
    CASE 
     WHEN onboardings.source_iyg is true THEN null
     WHEN FIRST_VALUE(campaign_name) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) is null THEN 'sin campaña' 
     ELSE FIRST_VALUE(campaign_name) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) 
     END AS  campaign_name,
    CASE 
     WHEN onboardings.source_iyg is true THEN null
     WHEN FIRST_VALUE(coupon_name) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) is null THEN 'sin cupón' 
     ELSE FIRST_VALUE(coupon_name) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) 
     END AS  coupon_name,
    CASE 
     WHEN onboardings.source_iyg is true THEN 'iyg'
     WHEN FIRST_VALUE(tipo_cupon) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) is null THEN 'sin campaña' 
     ELSE FIRST_VALUE(tipo_cupon) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) 
     END AS  tipo_cupon,
    CASE 
     WHEN onboardings.source_iyg is true THEN null
     WHEN FIRST_VALUE(coup_amount) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) is null THEN null
     ELSE FIRST_VALUE(coup_amount) OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) 
     END AS  coup_amount,  
    CASE 
     WHEN onboardings.source_iyg is true THEN 'iyg'
     WHEN FIRST_VALUE(status_cupon)  OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) is true then 'confirmado'
     WHEN  FIRST_VALUE(status_cupon)  OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC)  = false then 'ingresado' 
     ELSE 'sin cupón' 
     END AS status_cupon,
    CASE 
     WHEN onboardings.source_iyg is true THEN fecha_ob
     ELSE FIRST_VALUE(CAST(redeem_timestamp AS DATE))  OVER (PARTITION BY id_usuario ORDER BY redeem_timestamp ASC) 
     END AS fecha_redeem,
    LAST_VALUE(economics.fecha) OVER (PARTITION BY user ORDER BY economics.fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as ult_fecha_actividad, 
    is_pep
   FROM onboardings 
   LEFT JOIN first_cashins USING(id_usuario)
   LEFT JOIN activaciones USING(id_usuario)
   LEFT JOIN cupones USING (id_usuario)
   LEFT JOIN economics on user = id_usuario 
 
  
