{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH 
invita_y_gana as (
   SELECT DISTINCT
     --identificadores cliente y reward
     p.user,
     p.referrer ,
     p.reward,
     p.status,
     p.referral_type ,
     --Creación del referral prom
     p.created creacion_ref_prom,
     -- TRX de compra que disparan devoluciones
     mo.fecha_creacion mo_created_at_trx ,
     mo.impfac mo_amount_trx,
     mo.nomcomred nombre_trx,
     -- TRX de Reward
     mo_r.fecha_creacion mo_created_at ,
     mo_r.impfac mo_amount_reward,
     mo_r.nomcomred nombre_reward,
     p.redeemed redeemed_ref_prom,
   FROM   {{source('payment_loyalty','referral_prom')}} p --`tenpo-airflow-prod.payment_loyalty.referral_prom` p 
   LEFT JOIN {{ source('prepago', 'prp_movimiento') }} mo  ON (p.reference = mo.uuid) --{{source('prepago','prp_movimiento')}} mo #Para trx que gatillan una devolución
   LEFT JOIN {{ source('prepago', 'prp_movimiento') }} mo_r ON (p.reward = mo_r.uuid) --Para trx que disparan devoluciones
   LEFT JOIN {{ source('transactions_history', 'transactions_history') }}  th_r ON (th_r.reference_id = p.reward ) --Para trx que disparan devoluciones
   WHERE TRUE
     AND p.status in ('CONFIRMED')
     AND p.referral_type = 'REFERRED'
  ),
  
  ig_registrados as (
    SELECT DISTINCT
      uuid user
    FROM  {{ ref('funnel_invitagana') }} 
    WHERE paso = '7. OB exitoso'
  ),
  
  ig_first_cashin as (
        SELECT DISTINCT
          uuid user
        FROM  {{ ref('funnel_invitagana') }} 
        WHERE paso = '8. First Cashin'
  ),
  
  ig_activa as (
        SELECT DISTINCT
          uuid user
        FROM  {{ ref('funnel_invitagana') }} 
        WHERE paso = '9. Activa tarjeta'
  ),
  
  ig_pseudocompra as (
        SELECT DISTINCT
          uuid user,
          monto monto_pseudocompra
        FROM  {{ ref('funnel_invitagana') }} 
        WHERE paso like '%Compra efectuada%'
  ),
  
  ig_compra as (
        SELECT DISTINCT
          uuid user,
          monto monto_compra
        FROM  {{ ref('funnel_invitagana') }} 
        WHERE paso = '10. Compra exitosa'
  ),

  rfm as (
    SELECT DISTINCT 
      user,
      COALESCE ( segment_ult60d, 'unknown') segment, 
      recency,
      cuenta_trx_origen count_trx,
      monto_gastado_origen amount,
      rfmp_product,
      rfmp_product product ,
      score_ult60d score
    FROM {{source('tablones_analisis','tablon_rfmp_v2')}}  --{{source('tablones_analisis','tablon_rfmp')}}
    WHERE
       Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM {{source('tablones_analisis','tablon_rfmp_v2')}}  )
  ),
  
  oportunistas as (
    SELECT DISTINCT
      id user,
      oportunista 
    FROM {{ ref('oportunistas') }} 
    WHERE invita_gana = 'Si'
  )
   

  SELECT DISTINCT 
    par.referrer referidor,
    par.camada_ob_referrer, 
    grupo,
    recency,
    CAST(rfm.amount AS INT64)  monetary,
    count_trx frequency,
    rfm.product ,
    rfm.rfmp_product ,
    rfm.score,
    rfm.segment ,
    IF(par.score_referrer is null, rfm.score , par.score_referrer) score_referidor,
    IF(par.segment_referrer is null, rfm.segment, CAST(par.segment_referrer as STRING)) segment_referidor,
    IF(earned is null, 0, CAST(earned AS INT64)) monto_ganado,
    IF(users is null, 0, users) usuarios_invitados,
    IF(redeemed is null, 0, redeemed) invitaciones_redimidas,
    COUNT(DISTINCT reg.user ) ob_invitados,
    COUNT(DISTINCT fci.user ) fci_invitados,
    COUNT(DISTINCT a.user ) act_invitados,
    COUNT(DISTINCT pc.user ) pseudocompras_invitados,
    COUNT(DISTINCT c.user ) compras_invitados,
    COUNT(DISTINCT IF( oportunista is not null AND oportunista = 1, o.user, null)) oportunistas,
    SUM( IF(mo_amount_trx is not null, mo_amount_trx,  null )) ltv_invitados,
    SUM(monto_pseudocompra) sum_monto_pseudocompra,
    SUM(monto_compra) sum_monto_compra
  FROM {{ ref('parejas_iyg') }} par
  LEFT JOIN {{source('payment_loyalty','referral_user')}}  r ON par.referrer = r.user  --`tenpo-airflow-prod.payment_loyalty.referral_user`  r ON par.referrer = r.user
  LEFT JOIN rfm ON rfm.user = par.referrer
  LEFT JOIN invita_y_gana ig ON ig.referrer = par.referrer AND ig.user = par.user
  LEFT JOIN ig_registrados reg ON ig.user = reg.user
  LEFT JOIN ig_first_cashin fci ON ig.user = fci.user
  LEFT JOIn ig_activa a ON a.user = ig.user
  LEFT JOIn ig_pseudocompra pc ON pc.user = ig.user
  LEFT JOIN ig_compra c ON ig.user = c.user 
  LEFT JOIN oportunistas o ON o.user = reg.user
  WHERE 
    TRUE = TRUE
    AND camada_ob_referrer is not null
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
