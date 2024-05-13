{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT id,
created_at,
creator_transaction_id,
policy_reference_id,
products,
supplier,
supplier_policy_id,
supplier_policy_number,
updated_at,
user_id,
version,
canceller_transaction_id 
FROM {{ source('insurance', 'policy_information_history') }} 