{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

with tabla_business as (SELECT 
business_id,
created_at,
updated_at,
accountc,
bureau_data_id,
businesssii,
category,
nickname,
phone,
type,
website,
status,
social_media1,
social_media2,
social_media3,
tenpo_user_id

FROM {{source('business','business')}}
qualify row_number() over (partition by business_id order by updated_at desc ) = 1)

SELECT * FROM tabla_business