{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table',
    enabled=True,
    cluster_by = ["id_user", "campana"],
    partition_by = {'field': 'redeem_date', 'data_type': 'timestamp'},
  ) 
}}

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
  COALESCE(mo.numaut,null) as numero_auth,
  COALESCE(mo.es_comercio_presencial,false) as es_presencial,
  re.id_trx as trx_to_redeem,
  IF(mo.uuid is null, th.reference_id, mo.uuid)   as mov_trx_to_redeem, --parche para los que traen mo.uuid = null en prp
  th.reference_id as th_trx_to_redeem,
  IF(mo.monto is null, IF(th.total_currency_value < 0, th.total_currency_value*-1, th.total_currency_value) , mo.monto)  AS mov_amount_to_redeem, --parche para los que traen mo.monto = null en prp
  IF(th.total_currency_value < 0, th.total_currency_value*-1, th.total_currency_value) AS th_amount_to_redeem,
  IF(mo.fecha_creacion is null, th.created_at, mo.fecha_creacion) mo_fecha_to_redeem,
  th.created_at th_fecha_to_redeem,
  re.redeem_date,
  re.created_at,
  re.id_user,
  us.state state_user,
  re.confirmed,
  ca.name as campana,
  re.reconciled,
  co.name as coupon,
  IF(mo.nomcomred is null, th.description, mo.nomcomred) mo_nomcomred,
  ca.campaign_type_characterization as coupon_type,
  ca.campaign_objective_characterization as objective
FROM {{ref('campaigns')}} ca 
JOIN {{source('payment_loyalty','coupons')}} co ON ca.id=co.id_campaign
JOIN {{source('payment_loyalty','redeems')}} re ON re.id_coupON = co.id
LEFT JOIN {{source('prepago','prp_movimiento')}} mo ON (re.id_trx=mo.uuid) 
LEFT JOIN {{source('prepago','prp_movimiento')}} mo_r ON (re.id=mo_r.id_tx_externo) 
LEFT JOIN {{source('transactions_history','transactions_history')}} th_r ON (th_r.reference_id =re.id)
LEFT JOIN {{source('transactions_history','transactions_history')}} th ON (th.reference_id =re.id_trx)
LEFT JOIN {{ ref('users_tenpo') }}   us ON (us.id =re.id_user)