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
    ,row_number() OVER (PARTITION BY user, device_type ORDER BY event_time DESC) ro_num
    FROM(
      SELECT DISTINCT 
        customer_user_id user,
        device_type,
        event_time
      FROM {{source('appsflyer','organic_in_app_events_android')}} --`tenpo-external.appsflyer.organic_in_app_events_report_android_*`
      JOIN  {{ ref('users_tenpo') }}  ON customer_user_id = id
      WHERE 
        device_type is not null and  device_type != ''
        
      UNION ALL
      
      SELECT DISTINCT 
        customer_user_id user,
        device_type,
        event_time
      FROM {{source('appsflyer','organic_in_app_events_ios')}} --`tenpo-external.appsflyer.organic_in_app_events_report_ios_*`
      JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
      WHERE 
        device_type is not null  and  device_type != ''     
        
      UNION ALL
      
      SELECT DISTINCT 
        customer_user_id user,
        device_type,
        event_time
      FROM {{source('appsflyer','in_app_events_ios')}} -- `tenpo-external.appsflyer.in_app_events_report_ios_*`
      JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
      WHERE 
        device_type is not null and  device_type != ''
      
      UNION ALL
      
      SELECT DISTINCT 
        customer_user_id user,
        device_type,
        event_time
      FROM {{source('appsflyer','in_app_events_android')}} --`tenpo-external.appsflyer.in_app_events_report_android_*`
      JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
      WHERE 
        device_type is not null and  device_type != ''
      )
    ),
  last_device as (
    SELECT
      user
      ,LAST_VALUE(device_type) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_type
    FROM datos
    ),
  array_devices as (
    SELECT
      user
      ,TO_JSON_STRING(array_agg(device_type order by event_time)) array_unique_device_type
    FROM datos
    WHERE TRUE 
      AND ro_num = 1
    GROUP BY 
      1
      )
     
SELECT DISTINCT
  user
  ,array_unique_device_type
  ,lower(last_device_type) last_device_type
FROM array_devices
JOIN last_device USING(user)