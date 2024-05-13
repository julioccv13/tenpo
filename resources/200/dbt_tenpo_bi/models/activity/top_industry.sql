{{ config(materialized='table') }}


WITH
  economics_app AS (
    SELECT
      *
    FROM(
      SELECT 
        fecha, 
        nombre, 
        monto, 
        trx_id, 
        user, 
        linea, 
        canal, 
        comercio,
        rubro_recod rubro
      FROM {{ ref('economics') }} 
      WHERE 
        linea in ( 'mastercard', 'utility_payments' , 'top_ups')
              )
      ),  
    onboardings as (
      SELECT 
        id uuid,
        DATE(ob_completed_at, "America/Santiago") fecha_ob
      FROM {{ ref('users_tenpo') }} 
      WHERE 
        state in (4,7,8,21,22)
             ),      
    rubros_compras as (
      SELECT DISTINCT
        user,
        trx_id,
        rubro      
      FROM economics_app
      WHERE linea in ( 'mastercard', 'utility_payments' , 'top_ups')
      ),
      
    rubro_usuario as (
      SELECT DISTINCT 
        user tenpo_uuid
        ,COUNT(DISTINCT IF( rubro= 'Gamer', trx_id, null)) as GM
        ,COUNT(DISTINCT IF( rubro= 'Marketplace', trx_id, null)) as MK
        ,COUNT(DISTINCT IF( rubro= 'Delivery', trx_id, null)) as DM
        ,COUNT(DISTINCT IF( rubro= 'Otro', trx_id, null)) as OT
        ,COUNT(DISTINCT IF( rubro= 'Suscripciones', trx_id, null)) as SC
        ,COUNT(DISTINCT IF( rubro= 'Paypal', trx_id, null)) as PPL
        ,COUNT(DISTINCT IF( rubro= 'Pasarelas de pago', trx_id, null)) as PP
        ,COUNT(DISTINCT IF( rubro= 'Mercado Pago', trx_id, null)) as MP
        ,COUNT(DISTINCT IF( rubro= 'Tiendas x Depto', trx_id, null)) as TD
        ,COUNT(DISTINCT IF( rubro= 'Pago de Cuentas', trx_id, null)) as PC
        ,COUNT(DISTINCT IF( rubro= 'Juegos de Azar', trx_id, null)) as JA
        ,COUNT(DISTINCT IF( rubro= 'E-commerce Tecnológicos', trx_id, null)) as ET
        ,COUNT(DISTINCT IF( rubro= 'Telecomunicaciones', trx_id, null)) as TC
        ,COUNT(DISTINCT IF( rubro= 'Mejoramiento del Hogar', trx_id, null)) as MH
        ,COUNT(DISTINCT IF( rubro= 'Viajes y Entretención', trx_id, null)) as VE
        ,COUNT(DISTINCT IF( rubro= 'Transporte', trx_id, null)) as CT
        ,COUNT(DISTINCT IF( rubro= 'Educación', trx_id, null)) as ED
        ,COUNT(DISTINCT IF( rubro= 'Salud y Belleza', trx_id, null)) as SB
        ,COUNT(DISTINCT IF( rubro= 'Gobierno', trx_id, null)) as GB
        ,COUNT(DISTINCT IF( rubro= 'Supermercados', trx_id, null)) as SM
        ,COUNT(DISTINCT IF( rubro= 'Seguros', trx_id, null)) as SG
        ,MAX(CASE WHEN rubro ='Gamer'   THEN 1 ELSE 0 END) BOOL_GM
        ,MAX(CASE WHEN rubro ='Marketplace' THEN 1 ELSE 0 END) BOOL_MK
        ,MAX(CASE WHEN rubro ='Delivery'  THEN 1 ELSE 0 END) BOOL_DM
        ,MAX(CASE WHEN rubro ='Otro'  THEN 1 ELSE 0 END) BOOL_OT
        ,MAX(CASE WHEN rubro ='Suscripciones'  THEN 1 ELSE 0 END) BOOL_SC
        ,MAX(CASE WHEN rubro ='Paypal'  THEN 1 ELSE 0 END) BOOL_PPL
        ,MAX(CASE WHEN rubro ='Pasarelas de pago'  THEN 1 ELSE 0 END) BOOL_PP
        ,MAX(CASE WHEN rubro ='Mercado Pago'  THEN 1 ELSE 0 END) BOOL_MP
        ,MAX(CASE WHEN rubro ='Tiendas x Depto'  THEN 1 ELSE 0 END) BOOL_TD
        ,MAX(CASE WHEN rubro ='Pago de Cuentas'  THEN 1 ELSE 0 END) BOOL_PC
        ,MAX(CASE WHEN rubro ='Juegos de Azar' THEN 1 ELSE 0 END) BOOL_JA
        ,MAX(CASE WHEN rubro ='E-commerce Tecnológicos' THEN 1 ELSE 0 END) BOOL_ET
        ,MAX(CASE WHEN rubro ='Telecomunicaciones' THEN 1 ELSE 0 END) BOOL_TC
        ,MAX(CASE WHEN rubro ='Mejoramiento del Hogar' THEN 1 ELSE 0 END) BOOL_MH
        ,MAX(CASE WHEN rubro ='Viajes y Entretención' THEN 1 ELSE 0 END) BOOL_VE
        ,MAX(CASE WHEN rubro ='Transporte' THEN 1 ELSE 0 END) BOOL_CT
        ,MAX(CASE WHEN rubro ='Educación' THEN 1 ELSE 0 END) BOOL_ED
        ,MAX(CASE WHEN rubro ='Salud y Belleza'   THEN 1 ELSE 0 END) BOOL_SB
        ,MAX(CASE WHEN rubro ='Gobierno' THEN 1 ELSE 0 END ) BOOL_GB
        ,MAX(CASE WHEN rubro ='Supermercados'  THEN 1 ELSE 0 END) BOOL_SM
        ,MAX(CASE WHEN rubro ='Seguros'  THEN 1 ELSE 0 END) BOOL_SG
    FROM rubros_compras
    GROUP BY user
    ),
    
    rubros_bool as (
     SELECT DISTINCT 
        user tenpo_uuid

       FROM rubros_compras
     GROUP BY user
     )
--    SELECT * FROM rubros_bool  WHERE tenpo_uuid = '3225bdb1-a27e-4971-b1a3-8e18e8852515'
SELECT
  * EXCEPT(BOOL_GM,BOOL_MK,BOOL_DM,BOOL_OT,BOOL_SC,BOOL_PPL,BOOL_PP,BOOL_MP,BOOL_TD,BOOL_PC,BOOL_JA,BOOL_ET,BOOL_TC,BOOL_MH,BOOL_VE,BOOL_CT,BOOL_ED,BOOL_SB,BOOL_GB,BOOL_SM,BOOL_SG),
  GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) cnt_trx_en_rubro_top,
  CASE 
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = GM THEN  'GM'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = MK THEN  'MK'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = DM THEN  'DM'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = OT THEN  'OT'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = SC THEN  'SC'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = PPL THEN 'PPL'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = PP THEN  'PP'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = MP THEN  'MP'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = TD THEN  'TD'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = PC THEN  'PC'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = JA THEN  'JA'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = ET THEN  'ET'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = TC THEN  'TC'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = MH THEN  'MH'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = VE THEN  'VE'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = CT THEN  'CT'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = ED THEN  'ED'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = SB THEN  'SB'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = GB THEN  'GB'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = SM  THEN 'SM'
   WHEN GREATEST(GM, MK, DM, OT, SC, PPL, PP, MP, TD, PC, JA, ET, TC, MH, VE, CT, ED, SB, GB, SM,SG) = SG  THEN 'SG'

  ELSE null
  END rubro_top,
  BOOL_GM+BOOL_MK+BOOL_DM+BOOL_OT+BOOL_SC+BOOL_PPL+BOOL_PP+BOOL_MP+BOOL_TD+BOOL_PC+BOOL_JA+BOOL_ET+BOOL_TC+BOOL_MH+BOOL_VE+BOOL_CT+BOOL_ED+BOOL_SB+BOOL_GB+BOOL_SM+BOOL_SG cnt_unique_rubro,
FROM rubro_usuario 
