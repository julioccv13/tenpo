{{ 
  config(
    materialized='table',
    tags=["hourly", "bi"]
  ) 
}}


SELECT 
        DISTINCT
        IFNULL(B.motor,'desconocido') motor,
        DATE(r.redeem_date, "America/Santiago") fecha,
        r.redeem_date	AS trx_timestamp,
        -- r.id AS redeem_id, --id_tx_externo from prp
        -- prp.uuid as trx_id,
        r.id_user as user,
        ct.name as nombre,
        c.name as premio,
        ca.amount monto,
        CASE 
            WHEN valor_dolar_cierre IS null THEN round(ca.amount/714) 
        ELSE round(ca.amount/valor_dolar_cierre) END AS monto_usd,
FROM {{source('payment_loyalty','redeems')}} r
JOIN {{source('payment_loyalty','coupons')}} c on r.id_coupon = c.id
JOIN {{source('payment_loyalty','campaigns')}} ca on ca.id = c.id_campaign
JOIN {{source('payment_loyalty','campaign_type')}} ct on ct.id = ca.campaign_type_id
LEFT JOIN (SELECT 
   fecha,
   IF(valor_dolar_cierre is null, LAG(valor_dolar_cierre) OVER (ORDER BY fecha), 
   IF( (LAG(valor_dolar_cierre) OVER (ORDER BY fecha)) IS NULL, 714, valor_dolar_cierre )) valor_dolar_cierre
FROM {{ref('dolar')}}
order by fecha desc
LIMIT 1) ON 1 = 1
LEFT JOIN {{source('prepago','prp_movimiento')}} prp on prp.id_tx_externo = r.id
LEFT JOIN  {{ ref('appsflyer_users') }} B on r.id_user = B.customer_user_id
WHERE ct.name = 'GENERICA_FECHA_OB_CON_CASHIN'
AND confirmed = true