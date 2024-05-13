{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

select 
    *
FROM  {{source('tyba_payment','withdraws')}}