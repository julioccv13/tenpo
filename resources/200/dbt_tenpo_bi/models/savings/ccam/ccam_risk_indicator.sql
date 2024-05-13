{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}


WITH
  datos as (
    SELECT
      b.id, 
      b.tributary_identifier  AS RUT_CLIENTE, 
      b.first_name AS NOMBRES, 
      b.last_name AS APELLIDOS, 
      b.created_at AS INICIO_DE_OPERACIONES,
      DATE(b.updated_at , "America/Santiago") AS FECHA_ACTUALIZACION,
      DATE(b.onboarding_date , "America/Santiago") AS OB_DATE,
      b.commune COMUNA,
      b.job ACTIVIDAD_OFICIO, 
      b.profession PROFESION,
      "Natural" AS TIPO_DE_PERSONA, 
      b.nationality AS NACIONALIDAD, 
      b.address AS DIRECCION, 
      "Inversionista" AS TIPO_DE_CLIENTE,
      is_conservative_profile AS CATEGORIA_INVERSIONISTA,
      fund_origin AS ORIGEN_DEFONDOS, 
      is_cp_related AS FUNCIONARIO, 
      is_exposed_person AS PEP,
      is_special_client AS CLIENTE_SENSIBLE,
      cp_participant_id PARTICIPANT_ID,
      cp_account_id ACCOUNT_ID,
      onboarding_status OB_STATUS,
      DATE_DIFF(CURRENT_DATE(),DATE(b.created_at), DAY) antiguedad_comercial,
      us_person,
      us_tin,
      only_chile_residence,
      CASE
       WHEN (DATE_DIFF(CURRENT_DATE(),DATE(b.created_at), DAY) >= 365  AND DATE_DIFF(CURRENT_DATE(),DATE(onboarding_date), DAY) < 1095) THEN 55
       WHEN DATE_DIFF(CURRENT_DATE(),DATE(b.created_at), DAY) >= 1095 THEN 10
       WHEN DATE_DIFF(CURRENT_DATE(),DATE(b.created_at), DAY) < 365 THEN 100
       ELSE null
       END AS parametro_antiguedad,
      is_exposed_person,
      CASE
       WHEN is_exposed_person = true THEN 100
       WHEN is_exposed_person = false THEN 10
       ELSE 55
       END AS parametro_pep,
      b.nationality,
      CASE
       WHEN b.nationality is null THEN 55
       WHEN (b.nationality LIKE '%CHILE%' or b.nationality LIKE '%Chile%' or b.nationality LIKE '%chile%') THEN 10
       ELSE 100
       END AS parametro_nacionalidad,
      is_special_client,
      CASE
       WHEN is_special_client = true THEN 100
       WHEN is_special_client = false THEN 10
       ELSE 55
       END AS parametro_special_client,
       origin,
      tr.country as country_tax_residence,
      tr.tin as tin_tax_residence,
      b.date_of_birth,
     FROM {{ ref('users_savings') }} b -- `tenpo-airflow-prod.onboarding_savings.users` b
     JOIN {{ ref('users_tenpo') }}  u USING(id) --`tenpo-airflow-prod.users.users`
     LEFT JOIN {{ source('onboarding_savings','user_tax_residence') }} tr ON (b.id = tr.user_id)
     WHERE true
     QUALIFY row_number() over (partition by b.id order by b.updated_at desc) = 1
     ),
     
  calculo_score as (
    SELECT
      *,
      parametro_pep * 0.5 + parametro_nacionalidad * 0.25 + parametro_special_client * 0.5 + parametro_antiguedad * 0.25 as score_riesgo
    FROM datos ),
    
  onboardings as (
    SELECT
     id,
     email,
     DATE(ob_completed_at, "America/Santiago") fecha_ob,
     EXTRACT(month FROM DATE(ob_completed_at, "America/Santiago")) camada,
     CASE 
      WHEN state = 4 THEN 'OK'
      WHEN state = 7 THEN 'CUENTA CONGELADA'
      WHEN state = 8 THEN 'CUENTA CERRADA'
     END AS status_users
    FROM {{ ref('users_tenpo') }}  --`tenpo-airflow-prod.users.users` 
    WHERE 
      state in (4,7,8,21,22)
      ),
      
   saldo as (
   WITH 
    positions as (
      SELECT DISTINCT
        user_id id,
        current_fee_quantity * (SELECT amount FROM {{ source('payment_savings', 'fees') }}  ORDER BY created_date DESC LIMIT 1) saldo, --`tenpo-airflow-prod.payment_savings.fees`
        row_number() over (partition by user_id order by created_date asc) as ro_no
      FROM {{ source('payment_savings', 'user_position') }} --`tenpo-airflow-prod.payment_savings.user_position`
      )
      
     SELECT 
      * EXCEPT(ro_no)
     FROM positions
     WHERE 
      ro_no = 1
      )
    
    SELECT
      c.*,
     CASE
      WHEN score_riesgo <= 40 THEN 'BAJO'
      WHEN score_riesgo >= 70 THEN 'ALTO'
      WHEN score_riesgo < 70 AND score_riesgo > 40 THEN 'MEDIO'
      ELSE null
      END AS INDICADOR_RIESGO,
      o.status_users STATUS_USERS,
      IF(s.saldo is null, 0 , s.saldo) as ULT_SALDO
    FROM calculo_score c
    LEFT JOIN saldo s USING(id)
    LEFT JOIN onboardings o USING(id)