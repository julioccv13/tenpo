{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}

SELECT
  tributary_identifier RUT,
  DATE(u.created_at , "America/Santiago") INICIO_OPERACIONES,
  DATE(u.onboarding_date , "America/Santiago") OB_DATE,
  link,
  origin
FROM  {{ source('report_savings', 'user_us_person_document') }}  i --`tenpo-airflow-prod.report_savings.user_general_contract_signed`
JOIN {{ ref('users_savings') }} u  USING(id) --`tenpo-airflow-prod.onboarding_savings.users`
