{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH
    datos_onboarding as (
        SELECT
          id user,
          DATE(ob_completed_at, "America/Santiago") fecha_ob,
          DATETIME(ob_completed_at, "America/Santiago") timestamp_ob,
          state
        FROM {{ ref('users_tenpo') }}  
        --WHERE state in (4,7,8,21,22)
        WHERE cast(state as string) in ({{ "'"+(var('states_onboarding') | join("','"))+"'" }})
        ),
    
    parejas_iyg as (
        SELECT DISTINCT 
          i.tenpo_uuid referrer,
          o.fecha_ob as fecha_ob_referrer,
          FORMAT_DATE("%Y-%m-01", o.fecha_ob) camada_ob_referrer,
          i.fecha_carga fecha_ingreso_beta,
          IF(p.user is not null, DATE_DIFF( CURRENT_DATE("America/Santiago"),i.fecha_carga, DAY), null) tiempo_en_beta_referrer, 
          p.user,
          ob.fecha_ob fecha_ob_user,
          FORMAT_DATE("%Y-%m-01", ob.fecha_ob) camada_ob_user,
          DATE_DIFF(ob.fecha_ob, i.fecha_carga, DAY) tiempo_al_ob, 
          COALESCE(i.camada, 'desconocido') grupo,
          null segment_referrer, 
          null score_referrer    
        FROM {{source('firebase','iyg')}} i --`tenpo-bi.firebase.iyg`  i 
        LEFT JOIN  {{source('payment_loyalty','referral_prom')}} p ON i.tenpo_uuid = referrer --`tenpo-airflow-prod.payment_loyalty.referral_prom` p ON i.tenpo_uuid = referrer
        LEFT JOIN datos_onboarding o on  i.tenpo_uuid = o.user
        LEFT JOIN datos_onboarding  ob on p.user = ob.user
        -- WHERE o.state in (4,7,8,21,22)
        WHERE cast(o.state as string) in ({{ "'"+(var('states_onboarding') | join("','"))+"'" }})
          AND (
            -- ob.state in (4,7,8,21,22) 
            cast(ob.state as string) in ({{ "'"+(var('states_onboarding') | join("','"))+"'" }})
            OR ob.state is null)
   )

SELECT 
 * 
FROM parejas_iyg