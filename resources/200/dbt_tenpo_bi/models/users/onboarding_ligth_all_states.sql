/*
Usaremos el historico para el funnel
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
    {{ hash_sensible_data('phone') }} as phone,
    {{ hash_sensible_data('email') }} as email,
    azure_id,
    updated,
    created,
    email_provider
FROM {{source('tenpo_users','onboarding_PROD')}}
WHERE true
QUALIFY ROW_NUMBER() OVER (PARTITION BY phone, status ORDER BY created asc) = 1