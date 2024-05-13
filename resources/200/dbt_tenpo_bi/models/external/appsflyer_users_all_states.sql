{{ config( 
    tags=["hourly", "bi"],
    materialized='table')
}}

WITH
  pre_datos_appsflyer as (
    SELECT DISTINCT 
      customer_user_id user,
      media_source,
      campaign,
      event_time,
      COALESCE(b.state, -1) state
    FROM  {{source('appsflyer','installs_report_android')}} -- `tenpo-external.appsflyer.installs_report_android_*`  --
    LEFT JOIN {{ ref('users_tenpo') }} b  ON customer_user_id = id --  --{{ref('users_tenpo')}}
    WHERE 
      customer_user_id is not null OR customer_user_id   != ""  

    UNION ALL
    
    SELECT DISTINCT 
      customer_user_id user,
      media_source,
      campaign,
      event_time,
      COALESCE(b.state, -1) state
    FROM  {{source('appsflyer','installs_report_ios')}} -- `tenpo-external.appsflyer.installs_report_ios_*`  --
    LEFT JOIN {{ ref('users_tenpo') }} b  ON customer_user_id = id --  --{{ref('users_tenpo')}}
    WHERE 
      customer_user_id is not null OR customer_user_id   != ""  

    UNION ALL

    SELECT DISTINCT 
      customer_user_id user,
      'organic' as media_source,
      'organic' as campaign,
      event_time,
      COALESCE(b.state, -1) state
    FROM  {{source('appsflyer','organic_in_app_events_android')}} -- `tenpo-external.appsflyer.organic_in_app_events_report_android_*`  --
    LEFT JOIN {{ ref('users_tenpo') }} b  ON customer_user_id = id --  --{{ref('users_tenpo')}}
    WHERE 
      customer_user_id is not null OR customer_user_id   != ""     

    UNION ALL     
    
    SELECT DISTINCT 
      customer_user_id user,
      'organic' as media_source,
      'organic' as campaign,
      event_time,
      COALESCE(b.state, -1) state
    FROM {{source('appsflyer','organic_in_app_events_ios')}} --`tenpo-external.appsflyer.organic_in_app_events_report_ios_*` 
    LEFT JOIN {{ ref('users_tenpo') }}   b ON customer_user_id = id -- {{ref('users_tenpo')}}
    WHERE 
      customer_user_id is not null OR customer_user_id   != "" 
    
    UNION ALL      
    
    SELECT DISTINCT 
      customer_user_id user,
      media_source,
      campaign,
      event_time,
      COALESCE(b.state, -1) state
    FROM  {{source('appsflyer','in_app_events_ios')}} --`tenpo-external.appsflyer.in_app_events_report_ios_*` --
    LEFT JOIN {{ ref('users_tenpo') }}   b ON customer_user_id = id -- {{ref('users_tenpo')}}
    WHERE 
      customer_user_id is not null OR customer_user_id   != ""   
    
    UNION ALL
    
    SELECT DISTINCT 
      customer_user_id user,
      media_source,
      campaign,
      event_time,
      COALESCE(b.state, -1) state
    FROM {{source('appsflyer','in_app_events_android')}} --`tenpo-external.appsflyer.in_app_events_report_android_*` --
    LEFT JOIN {{ ref('users_tenpo') }}   b ON customer_user_id = id -- {{ref('users_tenpo')}}
    WHERE 
      customer_user_id is not null OR customer_user_id   != ""   
      
    UNION ALL
    
    SELECT DISTINCT 
      id user,
      'invita y gana'  media_source,
      'invita y gana' campaign,
      ob_completed_at event_time,
      COALESCE(b.state, -1) state
    FROM {{ ref('users_tenpo') }} b  --  {{ref('users_tenpo')}} 
    WHERE 
      source like "%IG_%"  
    ),
  datos as (
    SELECT DISTINCT
      user
      ,CASE WHEN b.motor is null OR motor = "" THEN "desconocido" ELSE b.motor END as motor
      ,media_source
      ,campaign
      ,event_time
      ,state
      ,row_number() OVER (PARTITION BY user, CASE WHEN b.motor is null OR motor = "" THEN "desconocido" ELSE b.motor END ORDER BY event_time DESC) ro_num
    FROM pre_datos_appsflyer a
    LEFT JOIN {{ ref('media_source_dict') }} b USING(media_source, campaign) --  `tenpo-bi-prod.external.media_source_dict`
    ),  
  last_motor as (
    SELECT
      user
      ,LAST_VALUE(media_source) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_media_source_raw
      ,LAST_VALUE(motor) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_media_source
      ,LAST_VALUE(event_time) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_event_time
      ,LAST_VALUE(campaign) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_campaign
      ,FIRST_VALUE(motor) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_media_source
      ,FIRST_VALUE(event_time) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_event_time
      ,FIRST_VALUE(campaign) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) first_campaign
    FROM datos
    ),
  array_motor as (
    SELECT
      user
      ,state
      ,TO_JSON_STRING(array_agg(motor order by event_time)) array_unique_media_source
      ,TO_JSON_STRING(array_agg(campaign order by event_time)) array_unique_campaign
    FROM datos
    WHERE TRUE 
      AND ro_num = 1
    GROUP BY 
      1,2
      )     
SELECT DISTINCT
  user
  ,state
  ,array_unique_media_source
  ,array_unique_campaign
  ,last_event_time
  ,last_media_source_raw
  ,last_media_source
  ,first_event_time
  ,first_media_source
  ,last_campaign
  ,first_campaign
FROM array_motor
JOIN last_motor USING(user)