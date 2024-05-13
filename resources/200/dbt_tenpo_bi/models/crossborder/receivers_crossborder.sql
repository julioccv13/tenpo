{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "status",
    partition_by = {'field': 'date_usr_created_at', 'data_type': 'date'},
  ) 
}}

select distinct
  id,
  created_at,
  country_code,
  nationality,
  status,
  document_type,
  date(created_at) as date_usr_created_at,
from {{source('crossborder','users')}} u
where is_receiver
qualify row_number() over (partition by id order by created_at desc)=1