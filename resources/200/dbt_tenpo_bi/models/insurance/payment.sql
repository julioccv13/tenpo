{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT id,created_at,description,external_id,external_reversed_id,invoice,p_amount,p_currency_code,p_currency_exchange_rate_code,p_exchange_rate,p_exchange_rate_amount,status,transaction_id,type
updated_at FROM {{ source('insurance', 'payment') }} 

