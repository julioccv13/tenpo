{{ 
  config(
    materialized='table',
  ) 
}}

WITH appsflyer_users_min_time AS
---------------------------------- androind in app events ---------------------------------------
(SELECT 
        min(event_time) event_time,
        customer_user_id,
FROM {{source('appsflyer','in_app_events_android')}} 
WHERE customer_user_id is not null
group by 2

UNION ALL 

(SELECT 
        min(event_time) event_time,
        customer_user_id
FROM {{source('appsflyer','organic_in_app_events_android')}} 
group by 2)
---------------------------------- ios in app events ---------------------------------------

UNION ALL 

(SELECT 
        min(event_time) event_time,
        customer_user_id
FROM {{source('appsflyer','in_app_events_ios')}} 
group by 2)


UNION ALL 

(SELECT 
        min(event_time) event_time,
        customer_user_id
FROM {{source('appsflyer','organic_in_app_events_ios')}} 
group by 2))

-------------------- appsflyer_users_min_time (tabla intermedia) --------------------
SELECT 
        DISTINCT 
        min(event_time) event_time ,
        customer_user_id 
FROM appsflyer_users_min_time
GROUP BY 2
ORDER BY 1 DESC