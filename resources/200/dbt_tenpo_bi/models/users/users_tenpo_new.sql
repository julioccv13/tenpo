{{ 
    config(
        materialized='table'
        ,tags=["hourly", "bi"]
        ,alias='users'
        ,enabled=False
        ,partition_by = { 'field': 'fecha_ob_chile', 'data_type': 'date' }
    )
}}

WITH users as (

    SELECT
      created_at,
      updated_at, 
      A.id user,
      A.ob_completed_at,
      A.email, 
      tributary_identifier,
      CAST(A.phone AS STRING) phone,
      first_name,
      states.state_name state,
      address,
      nationality,
      marital_status,
      gender,
      profession,
      category,
      source,
      date_of_birth, 
      case
          when cast(A.state as string) in ({{ "'"+(var('states_onboarding') | join("','"))+"'" }}) and tributary_identifier is not null and profession is not null and address is not null and category in ('B1','C1')
          then 'registro exitoso'
      else 'registro incompleto' 
      end as status_onboarding,
      'old' as version_ob,
    FROM {{ source('tenpo_users', 'users') }}  A
    LEFT JOIN `tenpo-bi-prod.seed_data.user_states` states on A.state = states.state
    LEFT JOIN {{ ref('users_onboarding_ligth_completed') }} B on A.phone = B.phone 
    WHERE B.phone IS NULL -- excluir los que estan en el nuevo onboarding

),

users_failed_ob as (

  WITH failed AS 
    
    (SELECT 
      created_at, 
      updated_at, 
      user_id user, 
      CAST(null AS TIMESTAMP) ob_completed_at, 
      email, 
      '' as tributary_identifier,
      CAST(A.phone AS STRING) phone,
      first_name, 
      states.state_name state,
      CAST(null AS STRING) as address,
      nationality,
      CAST(null AS STRING) as marital_status,
      CAST(null AS STRING) as gender,
      profession, 
      CAST(null AS STRING) as category,
      CAST(null AS STRING) as source,
      CAST(null AS TIMESTAMP) date_of_birth, 
      'registro incompleto' as status_onboarding,
      'old' as version_ob,

    FROM  {{ source('tenpo_users', 'users_failed_onboarding') }} A
    LEFT JOIN `tenpo-bi-prod.seed_data.user_states` states on A.state = states.state
    LEFT JOIN (
        SELECT 
          DISTINCT phone,
      FROM  {{source('tenpo_users','onboarding_PROD')}}
    ) D on CAST(A.phone AS STRING) = replace(D.phone,'+','')
    WHERE D.phone IS NULL 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY created_at DESC) = 1 -- eliminar los correos duplicados
    ) 

  SELECT 
        * 
  FROM failed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY phone ORDER BY created_at DESC) = 1 -- eliminar los telefonos duplicados
),

new_ob_completed AS (

    SELECT  
        A.created created_at
        ,B.updated_at
        ,A.user_id as user
        ,B.ob_completed_at
        ,A.email
        ,B.tributary_identifier
        ,CAST(A.phone AS STRING) phone
        ,B.first_name
        ,CASE when CAST(B.state AS STRING) is not null then states.state_name ELSE CAST(B.state AS STRING) END AS state
        ,B.address
        ,B.nationality
        ,B.marital_status
        ,B.gender
        ,B.profession
        ,B.category
        ,B.source
        ,B.date_of_birth
        ,case 
          when B.tributary_identifier is not null and B.profession is not null and B.address is not null and B.category in ('B1','C1') then 'registro exitoso'
        else 'registro incompleto' end as status_onboarding
        ,'new' as version_ob,
  FROM {{ ref('users_onboarding_ligth_completed') }} A 
  LEFT JOIN {{ source('tenpo_users', 'users') }} B ON A.user_id = B.id
  LEFT JOIN `tenpo-bi-prod.seed_data.user_states` states on B.state = states.state

),

pre_onboarding_old AS (

  SELECT 
      * 
  FROM users

  UNION ALL

  SELECT 
      * 
  FROM users_failed_ob

),

onboarding_old AS (

  WITH ob_old AS 
  
    (SELECT 
          * 
    FROM pre_onboarding_old
    QUALIFY row_number() over (partition by email order by updated_at desc) = 1 -- eliminar correos duplicados (nos quedamos con el mas actualizado)
    )

  SELECT
      * 
  FROM ob_old
  QUALIFY row_number() over (partition by phone order by created_at desc) = 1 -- eliminar telefonos duplicados (nos quedamos con el primer registro)


),

final_users AS 

(

  SELECT 
        * 
  FROM new_ob_completed 

  UNION ALL 

  SELECT 
        * 
  FROM onboarding_old

)

SELECT
  distinct  
    created_at,
    updated_at,
    user,
    date(ob_completed_at,"America/Santiago") fecha_ob_chile,
    ob_completed_at,
    {{ hash_sensible_data('email') }} as email,
    tributary_identifier,
    {{ hash_sensible_data('phone') }} as phone,
    {{ hash_sensible_data('first_name') }} as first_name,
    state,
    address,
    nationality,
    marital_status,
    gender,
    profession,
    category,
    source,
    date_of_birth,
    status_onboarding,
    version_ob

FROM final_users
where true
qualify row_number() over (partition by user order by created_at DESC) = 1 -- desduplicar: este caso pasa cuando migra de la tabla onboardings a users con el telefono cambiado