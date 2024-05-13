{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT * FROM {{ source('insurance', 'balance_history') }} 