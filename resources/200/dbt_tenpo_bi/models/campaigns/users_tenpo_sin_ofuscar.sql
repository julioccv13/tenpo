{{ config(materialized='ephemeral') }}

WITH 
  users as (
    SELECT
    id, 
    email as email, 
    '' as tributary_identifier, -- No creo que ya sea necesario el RUT en la base de BI
    phone as phone, 
    state, 
    level, 
    plan,
    region_code, 
    profession, 
    nationality, 
    country_code,
    agree_terms_conditions, 
    is_card_active, 
    is_tef_validated,  
    created_at, 
    updated_at, 
    date_of_birth,
    ob_completed_at,
    first_name as first_name,
    source
  FROM {{ source('tenpo_users', 'users') }}  --{{source('tenpo_users','users')}}
  ),
  
  users_failed_ob as (
    SELECT 
    user_id id, 
    email as email, 
    '' as tributary_identifier, -- No creo que ya sea necesario el RUT en la base de BI
    SAFE_CAST(phone AS STRING) as phone,
    state, 
    level,
    plan,
    region_code, 
    profession, 
    nationality, 
    country_code,
    agree_terms_conditions, 
    is_card_active, 
    CAST(null AS BOOL) is_tef_validated, 
    created_at, 
    updated_at, 
    CAST(null AS TIMESTAMP) date_of_birth, 
    CAST(null AS TIMESTAMP) ob_completed_at, 
    first_name, 
    CAST(null AS STRING) as source
  FROM  {{ source('tenpo_users', 'users_failed_onboarding') }} -- `tenpo-airflow-prod.users.users_failed_onboarding` 
  ),
  churn_app as (
    SELECT distinct
      user id
      ,case when churn is false then 0 else 1 end as churn
      ,case when cliente is false then 0 else 1 end as cliente
    FROM {{ref('last_churn')}}
    ),
  union_table as (
  SELECT DISTINCT 
    a.*,
    coalesce(churn, 0) churn, -- churn = 0 es cliente_ready
    coalesce(cliente, 0) cliente,
    row_number() over (partition by id order by updated_at desc) as row_no_actualizacion 
  FROM (
    SELECT * FROM users 
    UNION ALL  
    SELECT * FROM users_failed_ob 
    ) a
   LEFT JOIN churn_app USING(id)
    )
  
SELECT DISTINCT
* EXCEPT(row_no_actualizacion)
FROM union_table
WHERE row_no_actualizacion = 1
  
