{{ config( 
    tags=["daily", "bi"],
    materialized='table')
}}

with appsflyer as (
select distinct
  -- TODO: Pasar a tenpo_uuid
  customer_user_id,
  COALESCE(t1.motor,t2.motor) as motor
FROM {{ref('appsflyer_weebhooks_events')}} t1
  FULL JOIN {{ref('appsflyer_users')}} t2 USING (customer_user_id)
)
SELECT 
  customer_user_id,
  ob_completed_at,
  case 
    when motor	like '%recargas%' then 'Recargas'
    when motor	like '%paypal%' then 'Paypal'
    else motor
  end as source,
FROM appsflyer A
  JOIN {{ref('users_tenpo')}} B on A.customer_user_id = B.id  
where  B.state in (4,7,8,21,22)
  and (motor like '%recargas%' or motor like '%paypal%')
