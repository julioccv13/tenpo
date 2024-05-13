{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT id,
country,
order_code,
created_at,
notification_status,
payment_id,
performer,
policy_id,
retries,
status,
supplier_status,
supplier_transaction_id,
type,
updated_at,
user_id 
FROM {{ source('insurance', 'transaction') }} 