{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT id,
created_at,
creator_transaction_id,
effective_date,
products,
status,
supplier,
supplier_policy_id,
supplier_policy_number,
ab_balance_statement,
ab_accumulated_debt,
ab_accumulated_debt_currency_code,
ab_last_billing_date_paid,
ab_current_billing_date,
ab_next_billing_date,
ab_payment_period_type,
p_gross,
p_net,
p_tax,
p_currency_code,
disabled_date,
updated_at,
user_id,
category,
country,
canceller_transaction_id, FROM {{ source('insurance', 'policy') }} 

