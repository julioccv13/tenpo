{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}

select 
    *
FROM  {{source('savings_rules','rules')}}
where TRUE
qualify ROW_NUMBER() over (partition by id order by updated_at)=1