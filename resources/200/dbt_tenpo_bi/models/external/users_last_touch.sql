{{ 
  config(
    materialized='table',
  ) 
}}


WITH last_touch AS 
(SELECT
    DISTINCT  
    DATE(fecha_hora, "America/Santiago") as touch_date,
    email,
    utm_campaign,
    campaign_type,
    CURRENT_DATE() AS  today_date,
    DATE_DIFF(CURRENT_DATE(), DATE(fecha_hora, "America/Santiago"), DAY)  as days_difference,
    --### NEW CHANGE HERE ##----
    --utm_campaign AS campaign_id,
    ---  REVISO LA MENOR DIFERENCIA DESDE LA ULTIMA VEZ QUE FUE TOCADO HASTA EL DIA DE HOY --- (91 DAYS AGO VS 101 DAYS AGO)
    MIN(DATE_DIFF(CURRENT_DATE(), DATE(fecha_hora, "America/Santiago"), DAY)) OVER (PARTITION BY email ORDER BY DATE_DIFF(CURRENT_DATE(), DATE(fecha_hora, "America/Santiago"), DAY)) as days_since_last_touch
--FROM {{source('clevertap','events')}}
FROM {{ source('clevertap', 'events') }}
where event in ('Notification Viewed', 'Push Impressions') 
order by email desc, touch_date desc)

SELECT 
        DISTINCT 
        touch_date last_touch_date, 
        email, 
        today_date, 
        days_since_last_touch,
        --campaign_id, 
        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), DATE(ob_completed_at,"America/Santiago"), DAY) <= 7 THEN false
            WHEN days_since_last_touch > 1 THEN false
        ELSE TRUE END AS no_molestar,
        CASE WHEN DATE_DIFF(CURRENT_DATE(), DATE(ob_completed_at,"America/Santiago"), DAY) <= 7 THEN true -->> rule that new onboardings dont apply to the touch policy
             WHEN ob_completed_at is null then false -->> field ob_complet_at exists since september 2020
             else false end as condition_ob_date,
        DATE(ob_completed_at,"America/Santiago") ob_completed_at,
       
FROM last_touch
JOIN {{ source('tenpo_users', 'users') }} using (email)               -->> {{source('tenpo_users','users')}}using (email) // {{source('clevertap_audit','clevertap_users')}} 
JOIN {{ source('clevertap_audit', 'clevertap_users') }} using (email) -->> pre-validation that user profile exists in clevertap
where days_difference = days_since_last_touch -->> get date of last day touch
and ob_completed_at is not null
order by email desc

---- REVISAR LOS MODELOS INCREMENTALES DBT --- 
/*

ARRAY DE FECHAS - JOIN PARA TENER FOTOS - 
FOTOS POR DIA 


SI NO, POR AIRFLOW - PARA SACAR FOTO 

*/