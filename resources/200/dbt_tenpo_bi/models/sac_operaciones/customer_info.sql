{{ 
    config(
        materialized='table'
        ,tags=["daily", "bi"]
        ,project=env_var('DBT_PROJECT22', 'tenpo-datalake-sandbox')
        ,schema='users'
        ,alias='customers_info'
    )
}}


SELECT 
      DISTINCT
      A.created_at,
      A.ob_completed_at,
      A.id as user,
      B.email,
      B.rut,
      A.state,
      us.state_name,
      us.description,
      B.category,
      CASE WHEN B_2.user IS NULL THEN 'onboarding antiguo' ELSE 'onboarding nuevo' END AS tipo_onboarding,
      C.mail_blocking_user generador_del_bloqueo,
      C.message,
      D.fecha_hora as last_login,
      D.ct_app_v as last_app_version,
      CASE WHEN E_1.tipo IS NOT NULL THEN E_1.estado_tarjeta ELSE 'Inactivo' end as estado_tarjeta_virtual,
      CASE WHEN E_1.tipo IS NOT NULL THEN E_1.fecha_creacion_tarjeta ELSE NULL end as fecha_activacion_tarjeta_virtual,
      CASE  WHEN E_1.tipo = 'VIRTUAL' THEN E_1.red ELSE NULL END as tipo_tarjeta_virtual,
      CASE WHEN E_2.tipo IS NOT NULL THEN E_2.estado_tarjeta ELSE 'Inactivo' end as estado_tarjeta_fisica,
      CASE WHEN F.fecha_activacion IS NULL THEN NULL ELSE F.fecha_activacion END AS fecha_activacion_tarjeta_fisica,
      CASE WHEN F.user IS NULL THEN NULL ELSE 'MASTERCARD' END AS tipo_tarjeta_fisica,
      CASE WHEN F.delivery_status_id IS NULL THEN NULL ELSE F.delivery_status_id END AS estado_delivery_tarjeta_fisica,
      CASE WHEN F.status_name IS NULL THEN NULL ELSE F.status_name END AS subestado_correos_chile,

FROM {{ ref('users_tenpo') }} A 
LEFT JOIN {{ source('tenpo_users', 'users') }} B ON A.id = B.id
LEFT JOIN {{ref('onboarding_ligth')}} B_2 ON B_2.user = A.id
LEFT JOIN (

    SELECT 
      A.*, 
      B.state
    FROM {{ source('sac', 'lock_user') }} A 
    JOIN {{ source('tenpo_users', 'users') }} B on A.mail_lock_user = B.email

    WHERE true 
      QUALIFY ROW_NUMBER() OVER (PARTITION BY mail_lock_user ORDER BY A.date DESC ) = 1
    ORDER BY mail_lock_user	, date DESC

) C on C.mail_lock_user = B.email

LEFT JOIN (

  SELECT 
      *
  FROM {{ ref('logins') }}
  WHERE true
  QUALIFY ROW_NUMBER () OVER (PARTITION BY user ORDER BY fecha_hora DESC) = 1

) D on D.user = A.id

LEFT JOIN (

  WITH data AS  
  
  (SELECT
        DISTINCT
        t.fecha_creacion fecha_creacion_tarjeta,
        t.estado as estado_tarjeta,
        t.tipo,
        t.red,
        u.uuid as id_usuario,
        u.estado estado_user
  FROM {{ source('prepago', 'prp_cuenta') }} c
  JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
  JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
  WHERE red <> 'WHITE_LABEL'
    QUALIFY ROW_NUMBER () OVER (PARTITION BY u.uuid, red, tipo ORDER BY t.fecha_actualizacion DESC) = 1
  )

  SELECT 
        * 
  FROM data
  WHERE tipo = 'VIRTUAL'
) E_1 on E_1.id_usuario = A.id


LEFT JOIN (

    WITH data AS 

    (SELECT
            DISTINCT
            t.fecha_creacion fecha_creacion_tarjeta,
            t.fecha_actualizacion,
            t.estado as estado_tarjeta,
            t.tipo,
            t.red,
            u.uuid as id_usuario,
            u.estado estado_user
      FROM {{ source('prepago', 'prp_cuenta') }} c
      JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
      JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
      WHERE red <> 'WHITE_LABEL'
        QUALIFY ROW_NUMBER () OVER (PARTITION BY u.uuid, red, tipo ORDER BY t.fecha_actualizacion DESC) = 1
    ) 

    SELECT
          *
    FROM data
    WHERE tipo = 'PHYSICAL'

) E_2 on E_2.id_usuario = A.id


LEFT JOIN (

    SELECT 
        user,
        fecha_creacion,
        delivery_status_id,
        status_name,
        fecha_activacion
  FROM {{ ref('orders') }}
  WHERE true
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY fecha_creacion DESC) = 1
  
) F on F.user = A.id

LEFT JOIN `tenpo-bi-prod.seed_data.user_states` us ON us.state = A.state

--WHERE A.state IN (4,7,8,21,22)
---{{ source('tenpo_users', 'users') }}