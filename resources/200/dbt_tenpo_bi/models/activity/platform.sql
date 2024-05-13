{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "user"
  ) 
}}

WITH
  datos as (
    SELECT
      *
    ,row_number() OVER (PARTITION BY user, platform ORDER BY event_time DESC) ro_num
    FROM(
      SELECT DISTINCT 
        customer_user_id user,
        platform,
        event_time
      FROM {{source('appsflyer','organic_in_app_events_android')}} --`tenpo-external.appsflyer.organic_in_app_events_report_android_*`
      JOIN  {{ ref('users_tenpo') }}  ON customer_user_id = id
      WHERE 
        platform is not null and  platform != ''
        
      UNION ALL
      
      SELECT DISTINCT 
        customer_user_id user,
        platform,
        event_time
      FROM {{source('appsflyer','organic_in_app_events_ios')}} --`tenpo-external.appsflyer.organic_in_app_events_report_ios_*`
      JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
      WHERE 
        platform is not null  and  platform != ''     
        
      UNION ALL
      
      SELECT DISTINCT 
        customer_user_id user,
        platform,
        event_time
      FROM {{source('appsflyer','in_app_events_ios')}} -- `tenpo-external.appsflyer.in_app_events_report_ios_*`
      JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
      WHERE 
        platform is not null and  platform != ''
      
      UNION ALL
      
      SELECT DISTINCT 
        customer_user_id user,
        platform,
        event_time
      FROM {{source('appsflyer','in_app_events_android')}} --`tenpo-external.appsflyer.in_app_events_report_android_*`
      JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
      WHERE 
        platform is not null and  platform != ''
      )
    ),
  last_platforms as (
    SELECT
      user
      ,LAST_VALUE(platform) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_platform
    FROM datos
    ),
  array_platform as (
    SELECT
      user
      ,TO_JSON_STRING(array_agg(platform order by event_time)) array_unique_platform
    FROM datos
    WHERE TRUE 
      AND ro_num = 1
    GROUP BY 
      1
      )
     
SELECT DISTINCT
  user
  ,array_unique_platform
  ,last_platform
FROM array_platform
JOIN last_platforms USING(user)