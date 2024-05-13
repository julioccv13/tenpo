{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT id,percentage,policy_information_history_id,relationship FROM {{ source('insurance', 'beneficiary_history') }} 
