{{ config( 
    tags=["hourly", "bi"],
    materialized='table')
}}

WITH appsflyer_users AS

        (WITH appsflyer_users_temp AS
        ---------------------------------- androind in app events ---------------------------------------
        (SELECT 
                event_time,
                customer_user_id,
                appsflyer_id,
                media_source,
                event_name,
                campaign
        FROM {{source('appsflyer','in_app_events_android')}}  
        WHERE customer_user_id is not null

        UNION ALL 
        -------- organic android --------
        (SELECT 
                event_time,
                customer_user_id,
                appsflyer_id,
                'organic' as media_source,
                event_name,
                campaign
        FROM {{source('appsflyer','organic_in_app_events_android')}} )
        ---------------------------------- ios in app events ---------------------------------------

        UNION ALL 

        (SELECT 
                event_time,
                customer_user_id,
                appsflyer_id,
                media_source,
                event_name,
                campaign
        FROM {{source('appsflyer','in_app_events_ios')}} )

        -------- organic ios --------
        UNION ALL 

        (SELECT 
                event_time,
                customer_user_id,
                appsflyer_id,
                'organic' as media_source,
                event_name,
                campaign
        FROM {{source('appsflyer','organic_in_app_events_ios')}} ))

        -------------------- appflyer_users_t1 (tabla intermedia 1) --------------------
        SELECT 
                DISTINCT 
                customer_user_id,
                media_source,
                campaign,
                CASE WHEN lower(media_source) like '%web%'then 'organic' 
                        WHEN lower(media_source) like '%email%' then 'organic'
                        WHEN lower(media_source) like '%liftoff%' then 'paid media'
                        WHEN lower(media_source) like '%doubleclick%' then 'paid media'
                        WHEN lower(media_source) like '%partnership%' then 'partnership'
                        WHEN lower(media_source) like '%paypal%' then 'xsell_paypal'
                        WHEN lower(media_source) like '%recargas%' then 'xsell_recargas'
                        WHEN lower(media_source) like '%lemmonet%' then 'paid media'
                        WHEN lower(media_source) like '%rocketlab%' then 'paid media'
                        WHEN lower(media_source) like '%googleadwords%' then 'paid media'
                        WHEN lower(media_source) like '%twitter%' then 'paid media'
                        WHEN lower(media_source) like '%spotify%' then 'paid media'
                        WHEN lower(media_source) like '%none%' then 'organic'
                        WHEN lower(media_source) like '%appsflyer_sdk_test%' then 'organic'
                        WHEN lower(media_source) like '%twitch%' then 'paid media'
                        WHEN lower(media_source) like '%facebook%' then 'paid media'
                        WHEN lower(media_source) like '%social%' then 'organic'
                        WHEN lower(media_source) like '%typeform%' then 'organic'
                        WHEN lower(media_source) like '%partnership%' then 'partnership'
                        WHEN lower(media_source) like '%klare%' then 'partnership'
                        WHEN lower(media_source) like '%organic%' then 'organic'
                        WHEN lower(media_source) like 'xsell' and lower(campaign) like '%paypal%' then 'xsell_paypal' 
                        WHEN lower(media_source) like 'xsell' and lower(campaign) like '%recarga%' then 'xsell_recargas'
                        WHEN lower(media_source) like '%appsflyer_sdk_test_int%' then 'organic'
                        WHEN lower(media_source) like '%sdk_test%' then 'organic'
                        WHEN lower(media_source) like '%test%' then 'organic'
                        WHEN lower(media_source) like '%activecampaign%' then 'organic'
                        WHEN lower(media_source) like '%others%' then 'organic'
                        WHEN lower(media_source) like '%dia del ni%' then 'organic'
                        WHEN lower(media_source) like '%af_banner%' then 'organic'
                        WHEN lower(media_source) like '%doubleclick_int%' then 'paid media'
                        WHEN lower(media_source) like '%doubleclick_int%' then 'paid media'
                        WHEN lower(media_source) like '%digitalturbine%' then 'paid media'
                        WHEN lower(media_source) like '%rankmyapp%' then 'paid media'
                        WHEN lower(media_source) like '%adzealous%' then 'paid media'
                        WHEN lower(media_source) like '%medios masivos%' then 'paid media'
                        WHEN lower(media_source) like '%facebook_instant_article%' then 'paid media'
                        WHEN lower(media_source) like '%facebook_marketplace%' then 'paid media'
                        WHEN lower(media_source) like '%facebook_mobile_feed%' then 'paid media'
                        WHEN lower(media_source) like '%facebook_instream_video%' then 'paid media'
                        WHEN lower(media_source) like '%instagram_explore%' then 'paid media'
                        WHEN lower(media_source) like '%instagram_feed%' then 'paid media'
                        WHEN lower(media_source) like '%instagram_storie%' then 'paid media'
                        WHEN lower(media_source) like 'dbm' then 'paid media'
                        WHEN lower(media_source) like '%spotify%' then 'paid media'
                        WHEN lower(media_source) like '%liftoff%' then 'paid media'
                        WHEN lower(media_source) like '%gg_search%' then 'paid media'
                        WHEN lower(media_source) like '%partnership%' and lower(campaign) like '%compara%' then 'partnership'
                        WHEN lower(media_source) like '%partnership%' and lower(campaign) like '%maneki%' then 'partnership'
                        WHEN lower(media_source) like '%partnership%' and lower(campaign) like '%lemmonet%' then 'partnership'
                        WHEN lower(media_source) like '%partnership%' and lower(campaign) like '%descuentos rata%' then 'partnership'
                        WHEN lower(media_source) like '%partnership%' and lower(campaign) like '%selyt%' then 'selyt'
                        WHEN lower(media_source) like '%referidos%' then 'referidos'
                        WHEN lower(media_source) like '%restricted%' then 'restricted_channel' 
        ELSE 'desconocido' end as motor,
        FROM appsflyer_users_temp
        ORDER BY customer_user_id DESC),

appsflyer_weebhooks AS (SELECT DISTINCT
                                customer_user_id,
                                media_source_weebhook,
                                motor,
--FROM `tenpo-bi-prod.external.appsflyer_weebhooks_events`
FROM {{ ref('appsflyer_weebhooks_events') }}
),

users AS (SELECT
                DATE(ob_completed_at, "America/Santiago") date,
                id,
                source
          --FROM {{source('tenpo_users','users')}}
          FROM {{ source('tenpo_users', 'users') }}
          WHERE state in (4,7,8,21,22))

---------------------------------------- appflyer users ----------------------------------------
SELECT DISTINCT 
        date event_time,
        users.id customer_user_id,
        users.source,

        CASE 
        WHEN users.source LIKE '%IG_%' THEN 'invita y gana'
        WHEN max(appsflyer_users.motor) IS NULL THEN appsflyer_weebhooks.motor
        ELSE IFNULL(max(appsflyer_users.motor),'desconocido') END AS motor,
        
        CASE 
        WHEN users.source LIKE '%IG_%' THEN 'invita y gana'
        WHEN max(appsflyer_users.media_source) IS NULL THEN appsflyer_weebhooks.media_source_weebhook
        ELSE max(appsflyer_users.media_source) END AS media_source, 

         CASE 
        WHEN users.source LIKE '%IG_%' THEN 'invita y gana'
        ELSE max(appsflyer_users.campaign) END AS campaign, 

FROM users
LEFT JOIN appsflyer_users ON appsflyer_users.customer_user_id = users.id
LEFT JOIN appsflyer_weebhooks ON appsflyer_weebhooks.customer_user_id = users.id 
GROUP BY id, date, users.source, appsflyer_weebhooks.media_source_weebhook, appsflyer_weebhooks.motor
ORDER BY date DESC