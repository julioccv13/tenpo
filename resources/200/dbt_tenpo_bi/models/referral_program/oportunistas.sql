{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH 

  last_ndd_bm as (
    SELECT DISTINCT
      id,
      last_ndd_service_bm
    FROM {{ ref('users_allservices') }}
    WHERE state in (4,7,8,21,22)
  ),
  onboardings as ( 
    SELECT DISTINCT
      u.id, 
      u.state, 
      u.email, 
      DATETIME(u.ob_completed_at, "America/Santiago") timestamp_ob,
      DATETIME(u.updated_at, "America/Santiago") fecha_actualizacion,
      DATE(u.ob_completed_at, "America/Santiago")  fecha_ob, 
      CASE WHEN u.source like '%IG_%' THEN 'Si' ELSE 'No' END AS invita_gana,
      last_ndd_bm.last_ndd_service_bm,
      CASE 
       WHEN u.source like '%IG_%'  AND grupo is null THEN 'desconocido' 
       WHEN u.source not like '%IG_%' AND grupo is not null THEN null
       ELSE CAST(grupo AS STRING) 
       END AS grupo,
      camada_ob_referrer,
      camada_ob_user,
      u.churn
    FROM {{ ref('users_tenpo') }}  u 
    LEFT JOIN last_ndd_bm  ON u.id = last_ndd_bm.id
    LEFT JOIN {{ ref('parejas_iyg') }} p on p.user = u.id
    WHERE
      u.state in  (4,7,8,21,22)
    ),
  activacion as (
    SELECT DISTINCT
     uuid user,
     'activacion' as linea,
     FIRST_VALUE(timestamp_fecha) OVER (PARTITION BY uuid ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) primera_trx, 
     0 primer_monto
    FROM {{ ref('funnel_tenpo') }} 
    WHERE TRUE = TRUE
      AND lower(paso) like '%activa tarjeta%'
  ),
  primeras_trx as (
    SELECT DISTINCT
      user,
      linea,
      FIRST_VALUE(CAST(trx_timestamp AS DATETIME)) OVER (PARTITION BY user, linea ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) primera_trx,
      FIRST_VALUE(monto) OVER (PARTITION BY user, linea ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) primer_monto
    FROM {{ ref('economics') }}      
    WHERE 
      TRUE = TRUE
      AND linea not in ('reward', 'aum_savings')
    ),
  union_fechas_importantes as (
    SELECT DISTINCT
      user,
      MAX(IF(linea = 'activacion', primera_trx, null)) fecha_activacion,
      MAX(IF(linea = 'mastercard', primera_trx, null)) fecha_primera_trx_mc,
      MAX(IF(linea = 'mastercard', primer_monto , null)) monto_primera_trx_mc,
      MAX(IF(linea = 'cash_in', primera_trx, null)) fecha_primera_trx_ci,
      MAX(IF(linea = 'cash_in', primer_monto, null)) monto_primera_trx_ci,
      MAX(IF(linea = 'cash_out', primera_trx, null)) fecha_primera_trx_co,
      MAX(IF(linea = 'cash_out', primer_monto, null)) monto_primera_trx_co,
    FROM (
      SELECT 
        * 
      FROM primeras_trx 
      UNION ALL 
      SELECT
        * 
      FROM activacion )
    GROUP BY 1 
  ),
  usuarios_tenpo as ( 
    SELECT DISTINCT
      user id,
      IF(fecha_primera_trx_ci is not null, 'con first cashin', 'sin first cashin') carga_plata,
      fecha_primera_trx_ci  prim_cashin,
      DATETIME_DIFF(fecha_primera_trx_ci, timestamp_ob, HOUR) horas_a_primera_trx_ci,
      IF(fecha_activacion is not null, 'activa', 'no activa') activa_tarjeta,
      DATETIME_DIFF(fecha_activacion, timestamp_ob, HOUR) horas_a_activacion,
      IF(fecha_primera_trx_mc is not null, 'compra', 'no compra') compra,
      IF(fecha_primera_trx_co is not null, 'con first cashout', 'sin first cashout') retira_plata ,
      fecha_primera_trx_co prim_cashout,
      monto_primera_trx_co monto_primer_cashout,    
      fecha_primera_trx_mc prim_compra,
      monto_primera_trx_mc monto_primera_compra,
      DATETIME_DIFF(fecha_primera_trx_mc, timestamp_ob, HOUR) horas_a_primera_trx_mc
    FROM union_fechas_importantes
    JOIN onboardings ON user = id    
    ),   

  uso_productos as ( 
    SELECT DISTINCT
      user id,
      agrupacion agrupacion_prod_2,
      cuenta_trx_origen_mc trx_mc,
      cuenta_trx_origen_ci  trx_ci,
      cuenta_trx_origen_co trx_co,
      SAFE_DIVIDE(cuenta_trx_origen_co,cuenta_trx_origen_ci) ratio_coci
    FROM {{source('productos_tenpo','tenencia_productos_tenpo')}} --`tenpo-bi.productos_tenpo.tenencia_productos_tenpo` 
    WHERE Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM {{source('productos_tenpo','tenencia_productos_tenpo')}} ) ),
    
  ltv as ( 
    SELECT DISTINCT 
      user id,
      IF(monto_gastado_origen is null, 0, monto_gastado_origen) ltv,
      IF(cuenta_trx_origen is null , 0, cuenta_trx_origen) trx_totales,
      recency
    FROM {{source('tablones_analisis','tablon_rfmp_v2')}} --{{source('tablones_analisis','tablon_rfmp')}} 
    WHERE Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM {{source('tablones_analisis','tablon_rfmp_v2')}} )
    ),     
  premios_canjeados as (
    SELECT DISTINCT
      user id,
      COUNT(DISTINCT trx_id) total_premios,
      SUM(monto) monto_premios
    FROM {{ ref('economics') }} 
    WHERE TRUE
      AND linea = ('reward')
    GROUP BY 1
    ),
  motor_atribucion as ( 
     SELECT DISTINCT
       customer_user_id id,
       motor
     FROM {{ ref('appsflyer_users') }} 
     ),

  dedup as (   
    SELECT DISTINCT
       onboardings.*,
       CASE WHEN invita_gana = 'Si' THEN 'invitaygana' WHEN motor is null THEN 'desconocido' ELSE motor END AS motor, 
       usuarios_tenpo.* EXCEPT(id, horas_a_primera_trx_ci, horas_a_primera_trx_mc, horas_a_activacion),
       CASE WHEN horas_a_primera_trx_ci < 0 THEN 0 ELSE horas_a_primera_trx_ci END AS horas_a_primera_trx_ci, 
       CASE WHEN horas_a_primera_trx_mc < 0 THEN 0 ELSE horas_a_primera_trx_mc END AS horas_a_primera_trx_mc, 
       CASE WHEN horas_a_activacion < 0 THEN 0 ELSE horas_a_activacion END AS  horas_a_activacion,
       agrupacion_prod_2,
       trx_ci,
       trx_co,
       trx_mc,
       ratio_coci ,
       total_premios,
       monto_premios,
       ltv.ltv ,
       ltv.trx_totales ,
       ltv.recency recencia,
       IF((ltv.ltv <= 3000 and total_premios >= 2) 
           OR (ratio_coci >=  1 AND ltv.ltv < monto_premios) ,1,0) oportunista,
      row_number() over (partition by id order by fecha_actualizacion desc) as row_no_actualizacion 
    FROM onboardings
    LEFT JOIN usuarios_tenpo USING(id)
    LEFT JOIN motor_atribucion USING(id)
    LEFT JOIN uso_productos  USING(id)
    LEFT JOIN premios_canjeados USING(id)
    LEFT JOIN ltv USING(id)
    )

SELECT
  * EXCEPT(row_no_actualizacion)
FROM dedup
WHERE 
  row_no_actualizacion = 1