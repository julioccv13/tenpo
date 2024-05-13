/*
Usaremos el ultimo state para el calculo de ob ligths
*/

{{ 
  config(
    tags=["hourly", "bi"],
    materialized='ephemeral'
  ) 
}}

SELECT
    id,
    status,
    user_new,
    user_id AS user,
    phone,
    email,
    azure_id,
    updated,
    created,
    email_provider
FROM {{source('tenpo_users','onboarding_PROD')}}
WHERE true
QUALIFY ROW_NUMBER() OVER (PARTITION BY phone ORDER BY created desc) = 1
