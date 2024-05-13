{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}


SELECT
  tributary_identifier RUT,
  DATE(u.created_at, "America/Santiago") INICIO_OPERACIONES,
  DATE(onboarding_date, "America/Santiago") OB_DATE,
  front_id_link,
  back_id_link ,
  u.id,
  origin
FROM {{ source('onboarding_savings', 'user_identity_validation') }} i --`tenpo-airflow-prod.onboarding_savings.user_identity_validation` i
JOIN {{ ref('users_savings') }}  u on u.id = i.user_id    --`tenpo-airflow-prod.onboarding_savings.users`