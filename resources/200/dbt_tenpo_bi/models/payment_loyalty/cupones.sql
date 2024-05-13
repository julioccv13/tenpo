{{ 
  config(
    tags=["hourly", "bi"], 
    materialized='table',
    alias='coupons',
    partition_by = { 'field': 'fecha', 'data_type': 'date' }
  )
}}


SELECT * FROM {{ ref('cupones_cashin') }}

UNION ALL

SELECT * FROM {{ ref('cupones_crossborder') }}

UNION ALL

SELECT * FROM {{ ref('cupones_paypal') }}

UNION ALL 

SELECT * FROM {{ ref('cupones_purchases') }}

UNION ALL

SELECT * FROM {{ ref('cupones_top_ups') }}

UNION ALL 

SELECT * FROM {{ ref('cupones_utility_payment') }}

UNION ALL 

SELECT * FROM {{ ref('cupones_otros') }}


