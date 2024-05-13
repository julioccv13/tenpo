{{ config(tags=["hourly", "bi"], enabled=True ,materialized='table') }}
WITH  --  >>>>>>> REGISTROS EXITOSOS <<<<<<<<    
    ob as (
      SELECT DISTINCT
        user, 
        fecha_ob 
      FROM {{ ref('funnel_onboarding') }}   --`tenpo-bi.funnel.cp_funnel_onboarding`
      ),
         --  >>>>>>> COMPRAS <<<<<<<<  
    fci as ( 
      SELECT DISTINCT
        user, 
        fecha_fci 
      FROM {{ ref('funnel_fci') }} --`tenpo-bi.funnel.cp_usuarios_fci`  
      ),
    --  >>>>>>> COMPRAS <<<<<<<<  
    compras as ( 
      SELECT DISTINCT
        user, 
        fecha_ce 
      FROM  {{ ref('funnel_primera_compra') }}   --`tenpo-bi.funnel.cp_usuarios_primera_compra` 
      ),
    --  >>>>>>> TARJETAS ACTIVAS <<<<<<<<                  
    activaciones as (
      SELECT DISTINCT
          user, 
          fecha_activacion  
      FROM {{ ref('funnel_activacion') }} --`tenpo-bi.funnel.cp_usuarios_tarjeta`  
      ),
     --  >>>>>>> CUPONES DE ADQUISICIÓN/FIDELIZACION REDIMIDOS <<<<<<<<                   
     cupones as (
       SELECT DISTINCT
         user,
         id_redeem,
         campana,
         coupon,
         coupon_type,
         mo_fecha_redeem,
         mov_amount_redeem,
         mov_trx_to_redeem,
         th_trx_to_redeem,
         confirmed
       FROM {{ ref('coupons_trx_redeems') }} --`tenpo-bi.cupones.cp_cupones_redeems_trx` 
       WHERE 
        confirmed = true
        #AND objective = 'Adquisición'
        ),
    datos as (
      SELECT DISTINCT  
        user,
        CAST(fecha_ob AS DATE) fecha_ob,
        CAST(fecha_fci AS DATE) fecha_fci,
        CAST(fecha_activacion AS DATE) fecha_activ,
        CAST(fecha_ce AS DATE) fecha_ce,
        FIRST_VALUE(campana) OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) campaign_name ,
        FIRST_VALUE(coupon)  OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) coupon_name ,
        CASE 
         WHEN FIRST_VALUE(coupon_type) OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) is null then 'sin campaña' 
         ELSE FIRST_VALUE(coupon_type) OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) 
         END AS  tipo_cupon,
        FIRST_VALUE(mov_amount_redeem) OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) coup_amount,
        CASE 
         WHEN FIRST_VALUE(confirmed)  OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) is true then 'confirmado'
         WHEN  FIRST_VALUE(confirmed)  OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC)  = false then 'igresado' else 'sin cupón' 
         END AS status_cupon,
        FIRST_VALUE(CAST(mo_fecha_redeem AS DATE)) OVER (PARTITION BY user ORDER BY mo_fecha_redeem ASC) fecha_redeem,
      FROM ob 
      LEFT JOIN fci USING(user)
      LEFT JOIN activaciones USING(user)
      LEFT JOIN compras USING(user)
      LEFT JOIN cupones USING (user)
    )
                 
SELECT
  fecha_ob,
  fecha_fci,
  fecha_ce,
  CASE 
   WHEN coupon_name is null THEN 'sin cupón' 
   ELSE coupon_name END AS coupon_name,
  tipo_cupon,
  status_cupon,
  COUNT(DISTINCT IF(fecha_ob is not null, user, null)) onboardings, 
  COUNT(DISTINCT IF(fecha_fci is not null, user, null)) first_cashins, 
  COUNT(DISTINCT IF(fecha_activ is not null, user, null)) activaciones, 
  COUNT(DISTINCT IF(fecha_ce is not null, user, null)) compras, 
  COUNT(DISTINCT IF(status_cupon = 'confirmado', user, null)) confirmados,
  COUNT(DISTINCT IF(status_cupon = 'igresado', user, null)) ingresados,
FROM datos
GROUP BY 
  1,2,3,4,5,6                 