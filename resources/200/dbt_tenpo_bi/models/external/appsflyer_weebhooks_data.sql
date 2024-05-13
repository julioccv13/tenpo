{{ config( 
    tags=["hourly", "bi"],
    materialized='ephemeral')
}}

WITH users AS (

  SELECT    
      created_at,
      ob_completed_at,
      id as user,
      state,
      source
  FROM {{ ref('users_tenpo') }}

), weebhook_appsflyer AS (

    SELECT 
          *,
          left(right(json_query(json_string, "$.event_name"),length(json_query(json_string,  "$.event_name")) -1 ),length(right(json_query(json_string,  "$.event_name"),length(json_query(json_string, "$.event_name"))-1))-1) event_name,              
          left(right(json_query(json_string, "$.event_time"),length(json_query(json_string,  "$.event_time")) -1 ),length(right(json_query(json_string,  "$.event_time"),length(json_query(json_string, "$.event_time"))-1))-1) event_time,             
    FROM {{source('aux_table','appsflyer_events')}}
)

SELECT 
      users.created_at
      ,users.ob_completed_at
      ,users.user
      ,users.state
      ,users.source
      ,media_source
      ,campaign
      ,af_channel
      ,event_name
      ,event_time
FROM users
LEFT JOIN weebhook_appsflyer ON user = customer_user_id
WHERE true 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY event_time ASC) = 1
ORDER BY ob_completed_at DESC
