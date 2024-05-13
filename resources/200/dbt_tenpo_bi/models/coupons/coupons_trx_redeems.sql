{{ 
  config(
    materialized='table',
    enabled=True,
    tags=["hourly", "bi"],
    cluster_by = "campana",
    partition_by = {'field': 'redeem_date', 'data_type': 'timestamp'},
  ) 
}}

WITH 
  invita_y_gana as (
    SELECT DISTINCT
      --transacciones de devoluciones: REDEEM
      p.reward as id_redeem,
      mo_r.uuid mov_id_redeem, -- MOV: PRP_MOVIMIENTOS
      th_r.transaction_id th_id_redeem, -- MOV: TH_TRANSACTION_HISTORY
      mo_r.monto mov_amount_redeem,
      th_r.total_currency_value th_amount_redeem,
      mo_r.fecha_creacion mo_fecha_redeem,
      th_r.created_at th_fecha_redeem,
      --transacciones que disparan devoluciones: TO REDEEM
      p.reference trx_to_redeem,
      IF(mo.uuid is null, th.transaction_id, mo.uuid)  as mov_trx_to_redeem, --parche para los que traen mo.uuid = null en prp
      th.transaction_id as th_trx_to_redeem,
      IF(mo.monto is null, IF(th.total_currency_value < 0, th.total_currency_value*-1, th.total_currency_value) , mo.monto)  AS mov_amount_to_redeem, --parche para los que traen mo.monto = null en prp
      IF(th.total_currency_value < 0, th.total_currency_value*-1, th.total_currency_value) AS th_amount_to_redeem,
      IF(mo.fecha_creacion is null, th.created_at, mo.fecha_creacion) mo_fecha_to_redeem,
      IF(mo.nomcomred is null, th.description, mo.nomcomred) mo_nomcomred,
      th.created_at th_fecha_to_redeem,
      p.redeemed redeem_date,
      p.created created_at,
      p.user,
      us.state state_user,
      us.ob_completed_at as fecha_ob,
      IF(p.status = 'CONFIRMED', true, false) confirmed,
      IF(p.reconciliation = 'OK', true, false) reconciled,
      CAST(null as STRING) campaign_id,
      'INVITA Y GANA' as campana,
      CAST(null as STRING) coupon_id,
      'INVITA_GANA' as coupon,
      null as source, --uas.last_ndd_service_bm as source,
      'Cashback compra' as coupon_type,
      'Adquisición' as objective
    FROM {{source('payment_loyalty','referral_prom')}}  p 
    LEFT JOIN {{source('prepago','prp_movimiento')}}   mo ON (p.reference = mo.uuid) #Para trx que gatillan una devolución
    LEFT JOIN {{source('prepago','prp_movimiento')}}  mo_r ON (p.reward = mo_r.uuid) #Para trx que disparan devoluciones
    LEFT JOIN {{source('transactions_history','transactions_history')}}  th ON (th.reference_id   = p.reference)   #Para trx que gatillan una devolución
    LEFT JOIN {{source('transactions_history','transactions_history')}}  th_r ON  (th_r.transaction_id = p.reward )    #transacciones de devoluciones
    JOIN {{ ref('users_tenpo') }}   us ON (us.id = p.user)
    --JOIN {{ ref('users_allservices') }} uas ON (uas.id = us.id) 
    WHERE TRUE
      AND p.status in ( 'CONFIRMED')
      AND p.referral_type in ( 'REFERRED', 'REFERRER')
      ),
  cupones as (
    SELECT DISTINCT 
      ---transacciones de devoluciones: REDEEM
      re.id as id_redeem,
      mo_r.id_tx_externo mov_id_redeem, -- MOV: PRP_MOVIMIENTOS
      th_r.reference_id th_id_redeem, -- MOV: TH_TRANSACTION_HISTORY
      mo_r.monto mov_amount_redeem,
      th_r.total_currency_value th_amount_redeem,
      mo_r.fecha_creacion mo_fecha_redeem,
      th_r.created_at th_fecha_redeem,
      ---transacciones que disparan devoluciones: TO REDEEM
      re.id_trx as trx_to_redeem,
      IF(mo.uuid is null, th.reference_id, mo.uuid)   as mov_trx_to_redeem, --parche para los que traen mo.uuid = null en prp
      th.reference_id as th_trx_to_redeem,
      IF(mo.monto is null, IF(th.total_currency_value < 0, th.total_currency_value*-1, th.total_currency_value) , mo.monto)  AS mov_amount_to_redeem, --parche para los que traen mo.monto = null en prp
      IF(th.total_currency_value < 0, th.total_currency_value*-1, th.total_currency_value) AS th_amount_to_redeem,
      IF(mo.fecha_creacion is null, th.created_at, mo.fecha_creacion) mo_fecha_to_redeem,
      IF(mo.nomcomred is null, th.description, mo.nomcomred) mo_nomcomred,
      th.created_at th_fecha_to_redeem,
      re.redeem_date,
      re.created_at,
      re.id_user,
      us.state state_user,
      us.ob_completed_at as fecha_ob,
      re.confirmed,
      re.reconciled,

      ca.id campaign_id,
      ca.name as campana,
      co.id coupon_id,
      co.name as coupon,
      null as source, --uas.last_ndd_service_bm as source,
      ca.campaign_type_characterization coupon_type,
      ca.campaign_objective_characterization objective,
      
    FROM {{ref('campaigns')}}  ca 
      JOIN {{source('payment_loyalty','coupons')}} co ON ca.id=co.id_campaign
      JOIN {{source('payment_loyalty','redeems')}} re ON re.id_coupon = co.id
      LEFT JOIN {{source('prepago','prp_movimiento')}}  mo_r ON (re.id=mo_r.id_tx_externo) 
      LEFT JOIN {{source('prepago','prp_movimiento')}}  mo ON (re.id_trx=mo.uuid) 
      LEFT JOIN {{source('transactions_history','transactions_history')}} th_r ON (th_r.reference_id =re.id)
      LEFT JOIN {{source('transactions_history','transactions_history')}}  th ON (th.reference_id =re.id_trx)
      JOIN {{ ref('users_tenpo') }}   us ON (us.id =re.id_user)
      --JOIN {{ ref('users_allservices') }} uas ON (uas.id = us.id) 
    WHERE
      re.confirmed = true
      )
      
SELECT  *
FROM  invita_y_gana 
UNION ALL
SELECT * FROM cupones
  