{{ 
  config(
    tags=["hourly", "bi"], 
    materialized='table',
    partition_by = { 'field': 'fecha_creacion', 'data_type': 'date' }
  )
}}

WITH 
  users as (
    SELECT
    id, 
    {{ hash_sensible_data('email') }} as email, 
    tributary_identifier as tributary_identifier, -- No creo que ya sea necesario el RUT en la base de BI
    {{ hash_sensible_data('phone') }} as phone, 
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
    date(created_at) as fecha_creacion,
    updated_at, 
    date_of_birth,
    ob_completed_at,
    DATETIME(ob_completed_at, "America/Santiago") as ob_completed_at_dt_chile,
    {{ hash_sensible_data('first_name') }} as first_name,
    source,
    address, -- nuevos datos
    gender,
    validated,
    marital_status,
    category,
    region,
    commune
  FROM {{ source('tenpo_users', 'users') }}  --{{source('tenpo_users','users')}}
  ),
  
  users_failed_ob as (
    SELECT 
    user_id id, 
    {{ hash_sensible_data('email') }} as email, 
    '' as tributary_identifier, -- No creo que ya sea necesario el RUT en la base de BI
    {{ hash_sensible_data('phone') }} as phone,
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
    date(created_at) as fecha_creacion,
    updated_at, 
    CAST(null AS TIMESTAMP) date_of_birth, 
    CAST(null AS TIMESTAMP) ob_completed_at, 
    CAST(null AS DATETIME) ob_completed_at_dt_chile, 
    {{ hash_sensible_data('first_name') }}, 
    CAST(null AS STRING) as source,
    CAST(null AS STRING) as address,
    CAST(null AS STRING) as gender,
    CAST(null AS STRING) as validated,
    CAST(null AS STRING) as marital_status,
    CAST(null AS STRING) as category,
    CAST(null AS STRING) as region,
    CAST(null AS STRING) as commune,
  FROM  {{ source('tenpo_users', 'users_failed_onboarding') }} -- `tenpo-airflow-prod.users.users_failed_onboarding` 
  WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) = 1
  
  ),
  churn_app as (
    SELECT distinct
      user id
      ,case when churn is false then 0 else 1 end as churn
      ,case when cliente is false then 0 else 1 end as cliente
    FROM {{ref('last_churn')}}
    ),
  
   first_cashin as (
    SELECT 
      user as id,
      fecha as fecha_fci
    FROM {{ ref('cashin_fci') }}

   ),
  
  union_table as (
  SELECT DISTINCT 
    a.*,
    coalesce(churn, 0) churn, -- churn = 0 es cliente_ready
    coalesce(cliente, 0) cliente,
    first_cashin.fecha_fci,
    case 
      when cast(A.state as string) in ({{ "'"+(var('states_onboarding') | join("','"))+"'" }}) and tributary_identifier is not null  and address is not null  and profession is not null  and category in ('B1','C1') then 'completo'
    else 'incompleto' end as status_onboarding,
    row_number() over (partition by id order by updated_at desc) as row_no_actualizacion 
  FROM (
    SELECT * FROM users 
    UNION ALL  
    SELECT * FROM users_failed_ob 
    ) a
   LEFT JOIN churn_app USING(id)
   LEFT JOIN first_cashin USING (id)
    )
  
SELECT 
    DISTINCT * EXCEPT(row_no_actualizacion)
FROM union_table
WHERE row_no_actualizacion = 1
  
