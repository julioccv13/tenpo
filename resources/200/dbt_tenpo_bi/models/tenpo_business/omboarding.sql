{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

with tabla_omboarding as (SELECT 
created_at,
updated_at,
business_id,
businesssii,
status,
tenpo_user_id,
type
FROM {{source('business','onboarding')}}
qualify row_number() over (partition by business_id order by updated_at desc ) = 1)

SELECT * FROM tabla_omboarding