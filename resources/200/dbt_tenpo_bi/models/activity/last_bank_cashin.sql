{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "user"
  ) 
}}

-- TODO: Pasar a bancarization
SELECT
  *
FROM {{ ref('cashins_cca_and_robot') }}
WHERE TRUE
QUALIFY 
  ROW_NUMBER() OVER (PARTITION BY user ORDER BY created DESC) = 1
