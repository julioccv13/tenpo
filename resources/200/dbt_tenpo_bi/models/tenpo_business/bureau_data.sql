{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT id,
created_at,
updated_at,
city,
comuna,
has_business_sii,
members_count,
region,
sii_activities,
sii_name,
starting_activities_date,
status,
type,
FROM {{source('business','bureau_data')}}