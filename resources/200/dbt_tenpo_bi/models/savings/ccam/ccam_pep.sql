{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}


SELECT 
	u.tributary_identifier  RUT,
  u.first_name  NOMBRE,
  u.last_name  APELLIDOS, 
  p.relation  RELACION, 
  p.public_entity  ENTIDAD,
  DATE(u.created_at , "America/Santiago") INICIO_OPERACIONES,
  DATE(u.onboarding_date , "America/Santiago") OB_DATE,
	u.nationality ,"A" as Estatus ,
	"C" as Tipo,
  origin
FROM {{ ref('users_savings') }} u
JOIN {{ source('onboarding_savings', 'user_pep') }} p ON id = user_id
WHERE 
	is_exposed_person = true 
	AND onboarding_status = "FINISHED"