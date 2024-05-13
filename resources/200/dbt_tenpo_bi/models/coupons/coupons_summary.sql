{{ 
  config(
    materialized='table', 
    enabled=True,
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  coupons_campaigns as (
      SELECT
       *,
       IF( vigente = true , DATE_DIFF( CAST(CURRENT_DATETIME("America/Santiago")  AS DATE) , start_date , DAY ), duracion)  dias_acum,
      FROM(
          SELECT DISTINCT 
            DATE(campaign_start , "America/Santiago") as start_date,
            DATE(campaign_end  , "America/Santiago") as end_date,
            DATE(creation_date  , "America/Santiago" ) as creation_date,
            DATE_DIFF( DATE(campaign_end , "America/Santiago") , DATE(campaign_start , "America/Santiago"), DAY ) duracion,
            ca.id campaign_id,
            ca.name as campaign_name ,
            cp.id as coupon_id,
            cp.name as coupon_name,
            ca.amount,
            ca.max_amount,
            cp.quantity ,
            ca.state campaign_state,
            ca.campaign_type_characterization coupon_type,
            ca.campaign_objective_characterization objective,
            IF( (CAST(CURRENT_DATETIME("America/Santiago") as DATE) <=  DATE(campaign_end  , "America/Santiago") )
              AND (CAST(CURRENT_DATETIME("America/Santiago") as DATE) >=  DATE(campaign_start  , "America/Santiago"))  , TRUE, FALSE) as vigente
          FROM {{ref('campaigns')}}  ca 
          JOIN {{source('payment_loyalty','coupons')}} cp ON ca.id=cp.id_campaign
          JOIN {{source('payment_loyalty','redeems')}} re ON re.id_coupon = cp.id
          )
  ),

  redeems as(  
      SELECT DISTINCT
        cp.id as coupon_id,
        count(DISTINCT re.id) redimidos
      FROM {{source('payment_loyalty','campaigns')}}  ca 
      JOIN {{source('payment_loyalty','campaign_type')}} ct ON ct.id = ca.campaign_type_id 
      JOIN {{source('payment_loyalty','coupons')}} cp ON ca.id=cp.id_campaign
      JOIN {{source('payment_loyalty','redeems')}} re ON re.id_coupon = cp.id
      GROUP BY 1),

users_coupons AS(
    SELECT DISTINCT 
        id_redeem,
        mov_id_redeem,
        th_id_redeem,
        mov_amount_redeem,
        th_amount_redeem,
        ---transacciones que disparan devoluciones
        trx_to_redeem,
        mov_trx_to_redeem, 
        th_trx_to_redeem,
        mov_amount_to_redeem, 
        th_amount_to_redeem,
        mo_fecha_to_redeem,
        th_fecha_to_redeem,
        redeem_date,
        user,
        state_user,
        campaign_id,
        confirmed,
        reconciled,
    FROM {{ ref('coupons_trx_redeems') }}
    WHERE 
      confirmed is true
      AND coupon != 'INVITA_GANA' 
    )
  SELECT
    *,
    ROUND(SAFE_DIVIDE(quemados, redimidos),3) porc_quemados
  FROM(
      SELECT 
       cc.start_date inicio,
       cc.end_date fin,
       cc.campaign_name nombre_camp,
       cc.coupon_name nombre_cupon,
       cc.coupon_type tipo_cupon,
       cc.objective objetivo,
       cc.vigente,
       cc.campaign_state estado_ca,
       cc.duracion,
       cc.dias_acum,
       cc.amount monto,
       cc.max_amount max_monto,
       cc.quantity cantidad,
       IF(r.redimidos is null, 0, r.redimidos) redimidos,
       COUNT( DISTINCT IF( uc.confirmed = true ,id_redeem, null)) quemados,
       CAST(SAFE_DIVIDE(COUNT( DISTINCT IF( uc.confirmed = true ,id_redeem, null)),dias_acum) AS INT64) quemados_dia,
       IF(SUM(uc.mov_amount_redeem) is null , 0, CAST(SUM(uc.mov_amount_redeem) AS INT64)) costo,
       CAST(SAFE_DIVIDE(IF(SUM(uc.mov_amount_redeem) is null , 0, CAST(SUM(uc.mov_amount_redeem) AS INT64)),dias_acum) AS INT64) costo_dia,
      FROM users_coupons uc 
      JOIN coupons_campaigns cc ON uc.campaign_id = cc.campaign_id
      LEFT JOIN redeems r ON cc.coupon_id = r.coupon_id
      GROUP BY 
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
         )


