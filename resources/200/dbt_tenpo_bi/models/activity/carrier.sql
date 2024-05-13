{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "user"
  ) 
}}


WITH
  event_operator as (
     SELECT 
       id user,
       ct_carrier operator,
       e.fecha_hora event_time
     FROM {{source('clevertap','events')}} e -- TODO: Adri, estos JOINs se podrian hacer por customer ID o no?
     JOIN {{ ref('users_tenpo') }} u ON {{ hash_sensible_data('e.email') }} = u.email  
     WHERE
       event = "App Launched"
       AND e.email  in (SELECT DISTINCT email FROM {{source('clevertap_audit','clevertap_users')}})

     UNION ALL

     SELECT  
       customer_user_id user,
       operator,
       event_time
     FROM {{source('appsflyer','organic_in_app_events_android')}}
     JOIN  {{ ref('users_tenpo') }} ON customer_user_id = id
     WHERE 
       operator is not null and  operator != '' AND lower(operator) not in ('lavatelasmanos', '#cuidemonos', 'android','vivamosdesdecasa')
       
     UNION ALL
     
     SELECT  
       customer_user_id user,
       operator,
       event_time
     FROM {{source('appsflyer','organic_in_app_events_ios')}}
     JOIN {{ ref('users_tenpo') }}  ON customer_user_id = id
     WHERE 
       operator is not null  and  operator != '' AND lower(operator) not in ('lavatelasmanos', '#cuidemonos', 'android','vivamosdesdecasa') 
       
     UNION ALL
     
     SELECT  
       customer_user_id user,
       operator,
       event_time
     FROM  {{source('appsflyer','in_app_events_ios')}} --`tenpo-external.appsflyer.in_app_events_report_ios_*`
     JOIN {{ ref('users_tenpo') }} ON customer_user_id = id
     WHERE 
       operator is not null and  operator != '' AND lower(operator) not in ('lavatelasmanos', '#cuidemonos', 'android','vivamosdesdecasa')
     
     UNION ALL
     
     SELECT  
       customer_user_id user,
       operator,
       event_time
     FROM {{source('appsflyer','in_app_events_android')}} --`tenpo-external.appsflyer.in_app_events_report_android_*`
     JOIN {{ ref('users_tenpo') }}  ON customer_user_id = id
     WHERE 
       operator is not null and  operator != '' AND lower(operator) not in ('lavatelasmanos', '#cuidemonos', 'android','vivamosdesdecasa')
       ),
  datos as (
    
    SELECT
      *
      ,row_number() OVER (PARTITION BY user, operator ORDER BY event_time DESC) ro_num
    FROM (
      SELECT
        user
        ,event_time
        ,CASE
         WHEN REGEXP_CONTAINS(lower(operator), r'entel')  then 'entel'
         WHEN REGEXP_CONTAINS(lower(operator), r'movistar')  then 'movistar'
         WHEN REGEXP_CONTAINS(lower(operator), r'wom|h0m')  then 'wom'
         WHEN REGEXP_CONTAINS(lower(operator), r'claro')  then 'claro'
         WHEN REGEXP_CONTAINS(lower(operator), r'gtd')  then 'gtd'
         WHEN REGEXP_CONTAINS(lower(operator), r'vtr')  then 'vtr'
         WHEN REGEXP_CONTAINS(lower(operator), r'falabella')  then 'falabella'
         WHEN REGEXP_CONTAINS(lower(operator), r'vodafone')  then 'vodafone'
         WHEN REGEXP_CONTAINS(lower(operator), r'verizon')  then 'verizon'
         WHEN REGEXP_CONTAINS(lower(operator), r'simple')  then 'simple'
         WHEN REGEXP_CONTAINS(lower(operator), r'nextel')  then 'nextel'
         WHEN REGEXP_CONTAINS(lower(operator), r't-mobile')  then 't-mobile'
         WHEN REGEXP_CONTAINS(lower(operator), r'at&t')  then 'at&t' 
         WHEN REGEXP_CONTAINS(lower(operator), r'virgin ?mobile|virgin mobile|virgin')  then 'virgin_mobile'
         ELSE 'otro'
         END as operator
      FROM event_operator
      )
    ),
  last_device as (
    SELECT
      user
      ,LAST_VALUE(operator) OVER (PARTITION BY user ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_operator
    FROM datos
    ),
  array_operators as (
    SELECT
      user
      ,TO_JSON_STRING(array_agg(operator order by event_time)) array_unique_operator
      ,count(1) count_uniq_operator
    FROM datos
    WHERE TRUE 
      AND ro_num = 1
    GROUP BY 
      1
      )
     
SELECT DISTINCT
  user
  ,array_unique_operator
  ,count_uniq_operator
  ,last_operator
FROM array_operators
JOIN last_device USING(user)