{{ 
  config(
    materialized='table', 
    tags=["daily", "bi","datamart"],
    project=env_var('DBT_PROJECT8', 'tenpo-datalake-sandbox'),
    partition_by = { 'field': 'fecha', 'data_type': 'date' }
  ) 
}}

SELECT 
    date(created) fecha,
    *
FROM {{source('payment_loyalty','referral_prom')}}
