{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}



SELECT table1.business_id, DATE(table1.created_at, "America/Santiago") AS fecha, table1.type, table1.status as status_business, table2.status as status_onboarding
FROM (SELECT * FROM {{source('business','business')}}
qualify row_number() over (partition by business_id order by updated_at desc ) = 1) as table1
LEFT JOIN (SELECT * FROM {{source('business','onboarding')}}
qualify row_number() over (partition by business_id order by updated_at desc ) = 1) as table2
ON table1.business_id = table2.business_id
