{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table'
  ) 
}}

with datos as (
    select
        *
    FROM {{source('onboarding_savings','user_authorization')}}
    WHERE TRUE
    QUALIFY ROW_NUMBER() over (partition by user_id order by updated_at)=1
)
SELECT 
  count(distinct user_id) as cant_usuarios,
  date(created_at) as fecha
FROM datos
group by fecha
