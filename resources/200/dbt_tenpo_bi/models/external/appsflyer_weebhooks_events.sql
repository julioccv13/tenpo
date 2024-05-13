{{ config( 
    tags=["hourly", "bi"],
    materialized='table')
}}

SELECT  distinct
        A.customer_user_id, 
        A.media_source AS media_source_weebhook,
        CASE WHEN lower(media_source) like '%web%'then 'organic' 
                        WHEN lower(media_source) like '%email%' then 'organic'
                        WHEN lower(media_source) like '%liftoff%' then 'paid media'
                        WHEN lower(media_source) like '%doubleclick%' then 'paid media'
                        WHEN lower(media_source) like '%partnership%' then 'partnership'
                        WHEN lower(media_source) like '%paypal%' then 'xsell_paypal'
                        WHEN lower(media_source) like  '%xsell_recargas%' then 'xsell_recargas'
                        WHEN lower(media_source) like '%lemmonet%' then 'paid media'
                        WHEN lower(media_source) like '%rocketlab%' then 'paid media'
                        WHEN lower(media_source) like '%googleadwords%' then 'paid media'
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
                        WHEN lower(media_source) like '%referidos%' then 'referidos'
                        WHEN lower(media_source) like '%restricted%' then 'restricted_channel'
        ELSE 'desconocido' end as motor
--FROM `tenpo-bi.aux_table.appsflyer_events` A
FROM {{source('aux_table','appsflyer_events')}} A
--JOIN {{source('tenpo_users','users')}}B on B.id = A.customer_user_id
JOIN {{ source('tenpo_users', 'users') }} B on B.id = A.customer_user_id
order by 1 desc