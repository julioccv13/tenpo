
{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table'
  ) 
}}
SELECT
    *
FROM {{ source('payment_savings', 'cash_out') }} --`tenpo-airflow-prod.payment_savings.cash_out` 
WHERE is_total_cash_out is true