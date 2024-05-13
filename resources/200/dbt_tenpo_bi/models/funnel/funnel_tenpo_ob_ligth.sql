 {{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

 -->>>>> INICIO ONBOARDING <<<<<<< 
WITH usuarios_inicio_ob_ligth as (
  
  SELECT 
    DISTINCT
        DATETIME(A.created, 'America/Santiago')  as timestamp_fecha,
        DATE(A.created, 'America/Santiago')  as fecha,
        DATE(A.created, 'America/Santiago')  as fecha_funnel,
        DATE(NULL) AS fecha_ob,
        --DATE(B.ob_completed_at , 'America/Santiago')  as fecha_ob,
    '1. Inicio OB ligth'  as paso,
    1 paso_num,
    --A.phone as id,
    IFNULL(B.id,A.phone) as id,
    B.source ,
    0 monto
  FROM {{ref('onboarding_ligth_created_at')}} A -- `tenpo-airflow-prod.users.onboarding_PROD` A
  LEFT JOIN {{ref('users_tenpo')}} B ON A.phone = B.phone -- `tenpo-bi-prod.users.users_tenpo` B
  WHERE true 
  AND A.status IN ('CREATED','PHONE_SAVED','PHONE_VERIFIED','EMAIL_SAVED','EMAIL_VERIFIED','PASSWORD_SAVED','COMPLETED')
  AND A.phone IS NOT NULL

),

 -->>>>> GUARDA TELEFONO <<<<<<< 
 guarda_telefono as (

  SELECT 
    DISTINCT
        DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(A.created, 'America/Santiago')  as fecha,
        DATE(A.created, 'America/Santiago')  as fecha_funnel,
        DATE(NULL) AS fecha_ob,
        --DATE(B.ob_completed_at , 'America/Santiago')  as fecha_ob,
    '2. Guarda teléfono'  as paso,
    2 paso_num,
    --A.phone as id,
    IFNULL(A.user,A.phone) as id,
    B.source ,
    0 monto
  FROM {{ref('onboarding_ligth_all_states')}} A -- `tenpo-airflow-prod.users.onboarding_PROD` A
  LEFT JOIN {{ref('users_tenpo')}} B ON A.user = B.id -- `tenpo-bi-prod.users.users_tenpo` B
  LEFT JOIN {{ref('onboarding_ligth_created_at')}} C on A.phone = C.phone
  WHERE true 
  AND A.status IN ('PHONE_SAVED','PHONE_VERIFIED','EMAIL_SAVED','EMAIL_VERIFIED','PASSWORD_SAVED','COMPLETED')
  AND A.phone IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A.phone ORDER BY A.created DESC) = 1

),

-->>>>> VERIFICA TELEFONO <<<<<<< 
 valida_telefono as(

  SELECT 
    DISTINCT
        DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(A.created, 'America/Santiago')  as fecha,
        DATE(A.created, 'America/Santiago')  as fecha_funnel,
        DATE(NULL) AS fecha_ob,
        --DATE(B.ob_completed_at , 'America/Santiago')  as fecha_ob,
    '3. Valida teléfono'  as paso,
    3 paso_num,
    --A.phone as id,
    IFNULL(A.user,A.phone) as id,
    B.source ,
    0 monto
  FROM {{ref('onboarding_ligth_all_states')}} A -- `tenpo-airflow-prod.users.onboarding_PROD` A
  LEFT JOIN {{ref('users_tenpo')}} B ON A.user = B.id -- `tenpo-bi-prod.users.users_tenpo` B
  LEFT JOIN {{ref('onboarding_ligth_created_at')}} C on A.phone = C.phone
  WHERE true 
  AND A.status IN ('PHONE_VERIFIED','EMAIL_SAVED','EMAIL_VERIFIED','PASSWORD_SAVED','COMPLETED')
  AND A.phone IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A.phone ORDER BY A.created DESC) = 1
),

 -->>>>> GUARDA CORREO <<<<<<< 
 guarda_correo as (

    SELECT 
    DISTINCT
        DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(A.created, 'America/Santiago')  as fecha,
        DATE(A.created, 'America/Santiago')  as fecha_funnel,
        DATE(NULL) AS fecha_ob,
        --DATE(B.ob_completed_at , 'America/Santiago')  as fecha_ob,
    '4. Guarda correo'  as paso,
    4 paso_num,
    --A.phone as id,
    IFNULL(A.user,A.phone) as id,
    B.source ,
    0 monto
  FROM {{ref('onboarding_ligth_all_states')}} A -- `tenpo-airflow-prod.users.onboarding_PROD` A
  LEFT JOIN {{ref('users_tenpo')}} B ON A.user = B.id -- `tenpo-bi-prod.users.users_tenpo` B
  LEFT JOIN {{ref('onboarding_ligth_created_at')}} C on A.phone = C.phone
  WHERE true 
  AND A.status IN ('EMAIL_SAVED','EMAIL_VERIFIED','PASSWORD_SAVED','COMPLETED')
  AND A.phone IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A.phone ORDER BY A.created DESC) = 1

),

 -->>>>> VALIDA CORREO <<<<<<< 
 valida_correo as (

    SELECT 
    DISTINCT
        DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(A.created, 'America/Santiago')  as fecha,
        DATE(A.created, 'America/Santiago')  as fecha_funnel,
        DATE(NULL) AS fecha_ob,
        --DATE(B.ob_completed_at , 'America/Santiago')  as fecha_ob,
    '5. Valida correo'  as paso,
    5 paso_num,
    --A.phone as id,
    IFNULL(A.user,A.phone) as id,
    B.source ,
    0 monto
  FROM {{ref('onboarding_ligth_all_states')}} A -- `tenpo-airflow-prod.users.onboarding_PROD` A
  LEFT JOIN {{ref('users_tenpo')}} B ON A.user = B.id -- `tenpo-bi-prod.users.users_tenpo` B
  LEFT JOIN {{ref('onboarding_ligth_created_at')}} C on A.phone = C.phone
  WHERE true 
  AND A.status IN ('EMAIL_VERIFIED','PASSWORD_SAVED','COMPLETED')
  AND A.phone IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A.phone ORDER BY A.created DESC) = 1

),

 -->>>>> OB LIGTH EXITOSO <<<<<<< 
 ob_ligth_exitoso as (

  SELECT 
    DISTINCT
        DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(A.created, 'America/Santiago')  as fecha,
        DATE(A.created, 'America/Santiago')  as fecha_funnel,
        DATE(A.created, 'America/Santiago')  as fecha_ob,
    '6. Completa OB ligth'  as paso,
    6 paso_num,
    IFNULL(A.user,A.phone) as id,
    B.source ,
    0 monto
  FROM {{ref('onboarding_ligth')}}  A -- `tenpo-airflow-prod.users.onboarding_PROD` A
  LEFT JOIN {{ref('users_tenpo')}} B ON A.user = B.id -- `tenpo-bi-prod.users.users_tenpo` B
  LEFT JOIN {{ref('onboarding_ligth_created_at')}} C on A.phone = C.phone_no_hash
  WHERE true 
  AND A.status IN ('COMPLETED')
  AND A.phone IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY A.email ORDER BY A.updated asc) = 1
  

),

 -->>>>> INGRESA RUT CORREO (EQUIFAX) <<<<<<< 
 usuarios_rut as (

  SELECT DISTINCT
    DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
    DATE(B.created , 'America/Santiago')  as fecha,
    DATE(B.created , 'America/Santiago')  as fecha_funnel,
    DATE(A.ob_completed_at , 'America/Santiago')  as fecha_ob,
    '7. Ingresa RUT'  as paso,
    7 paso_num,
    B.user as id, 
    A.source,
    0 monto
  FROM {{ref('users_tenpo')}} A 
  JOIN {{ref('onboarding_ligth')}}  B ON B.user = A.id
  JOIN {{ref('onboarding_ligth_created_at')}} C on B.phone = C.phone_no_hash
  --WHERE state in (4,8,7,21,22)
  WHERE B.status = 'COMPLETED'  
  AND A.tributary_identifier IS NOT NULL
),


 -->>>>> INGRESA OCUPACION <<<<<<< 
ingresa_ocupacion as (
   SELECT
        DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(B.created , 'America/Santiago')  as fecha,
        DATE(B.created , 'America/Santiago')  as fecha_funnel,
        DATE(A.ob_completed_at , 'America/Santiago')  as fecha_ob,
      '8. Ingresa ocupación' as paso,
      8 paso_num,
      B.user as id,
      A.source,
      0 monto
   FROM {{ref('users_tenpo')}} A 
   JOIN {{ref('onboarding_ligth')}} B ON B.user = A.id
   JOIN {{ref('onboarding_ligth_created_at')}} C on B.phone = C.phone_no_hash
   --WHERE state in (4,8,7,21,22)
   WHERE B.status = 'COMPLETED'
   AND A.tributary_identifier IS NOT NULL 
   AND A.profession IS NOT NULL
),

ingresa_direccion as (
    SELECT
      DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(B.created , 'America/Santiago')  as fecha,
        DATE(B.created , 'America/Santiago')  as fecha_funnel,
        DATE(A.ob_completed_at , 'America/Santiago')  as fecha_ob,
      '9. Ingresa direccion'  as paso,
      9 paso_num,
      B.user as id,
      A.source,
      0 monto
    FROM {{ref('users_tenpo')}} A 
    JOIN {{ref('onboarding_ligth')}} B ON B.user = A.id
    JOIN {{ref('onboarding_ligth_created_at')}} C on B.phone = C.phone_no_hash
    --WHERE state in (4,8,7,21,22)
    WHERE B.status = 'COMPLETED'
    AND A.tributary_identifier IS NOT NULL 
    AND A.profession IS NOT NULL
    AND A.address IS NOT NULL
    
    ), 

  -->>>>>ONBOARDING EXITOSO<<<<<<< 
  usuarios_ob_exitoso as (

  WITH pre_dataset AS (
    SELECT
      DATETIME(C.created, 'America/Santiago')  as timestamp_fecha,
        DATE(B.created , 'America/Santiago')  as fecha,
        DATE(C.created , 'America/Santiago')  as fecha_funnel,
        DATE(A.ob_completed_at , 'America/Santiago')  as fecha_ob,
      '10. OB exitoso'  as paso,
      10 paso_num,
      A.id,
      A.source,
      0 monto,
      B.email,
      B.updated,
      B.phone
    FROM {{ref('onboarding_ligth')}} B 
    JOIN {{ref('users_tenpo')}} A on B.user = A.id
    -- FROM {{ref('users_tenpo')}} A 
    -- JOIN {{ref('onboarding_ligth')}} B ON B.user = A.id
    JOIN {{ref('onboarding_ligth_created_at')}} C on B.phone = C.phone_no_hash
    --WHERE state in (4,8,7,21,22) 
    WHERE B.status = 'COMPLETED'
    AND A.tributary_identifier IS NOT NULL 
    AND A.profession IS NOT NULL
    AND A.address IS NOT NULL
    AND A.category in ('B1', 'C1')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY B.user ORDER BY B.created desc) = 1
  ), dataset AS 

  (SELECT
        * --EXCEPT (email, updated)
  FROM pre_dataset
  QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated desc) = 1
  )

  SELECT 
      * EXCEPT (email, updated,phone)
  FROM dataset
  QUALIFY ROW_NUMBER() OVER (PARTITION BY phone ORDER BY updated desc) = 1

),

 -->>>>>FIRST CASHIN <<<<<<< 
  usuarios_ci as (
    SELECT DISTINCT
      DATETIME(D.created, 'America/Santiago')  as timestamp_fecha,
      FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago")ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha,
      FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
      DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
      '11. First Cashin'  as paso,
      11 paso_num,
      u.uuid,
      us.source,
      FIRST_VALUE(m.impfac) 
          OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as monto,  
    FROM {{ source('prepago', 'prp_cuenta') }} c
    JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
    JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
    JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
    JOIN {{ref('users_tenpo')}} us ON us.id = u.uuid
    JOIN {{ref('onboarding_ligth')}} A ON us.id = A.user -- `tenpo-airflow-prod.users.onboarding_PROD` A
    JOIN {{ref('onboarding_ligth_created_at')}} D ON A.phone = D.phone_no_hash
    WHERE
      m.estado    = 'PROCESS_OK' 
      AND indnorcor  = 0 
      AND tipofac in (3001,3002)
      AND A.phone IS NOT NULL
    ),
  -->>>>>TARJETA ACTIVA <<<<<<< 
  usuarios_tarjetas as (
  
  SELECT DISTINCT
    DATETIME(D.created, 'America/Santiago')  as timestamp_fecha,
    FIRST_VALUE(DATE(t.fecha_creacion ,  "America/Santiago" ) ) OVER (PARTITION BY u.uuid ORDER BY DATE(t.fecha_creacion ,  "America/Santiago" ) ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha, 
    FIRST_VALUE(DATE(t.fecha_creacion, "America/Santiago"))
      OVER (PARTITION BY u.uuid ORDER BY DATE(t.fecha_creacion ,  "America/Santiago" ) ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '12. Activa tarjeta' as paso,
    12 paso_num,
    u.uuid,
    us.source,
    0 monto
  FROM {{ source('prepago', 'prp_cuenta') }} c
  JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
  JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
  LEFT JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
  JOIN {{ref('users_tenpo')}} us ON us.id = u.uuid
  JOIN {{ref('onboarding_ligth')}} A ON us.id = A.user
  JOIN {{ref('onboarding_ligth_created_at')}} D ON A.phone = D.phone_no_hash
  WHERE 
    TRUE 
     AND A.phone IS NOT NULL
    AND c.estado = 'ACTIVE' 
    AND u.estado = 'ACTIVE'
    AND t.estado IN  ('ACTIVE', 'LOCKED')      
    AND ((tipofac in (336) AND m.estado  in ('PROCESS_OK') AND m.estado_de_negocio  in ('CONFIRMED','OK') AND id_tx_externo not like 'MC_%' AND indnorcor  = 0)
     OR (m.estado = 'PROCESS_OK' AND indnorcor  = 0 AND tipofac in (3001,3002) ) 
     OR (m.estado = 'PROCESS_OK' AND indnorcor  = 0 AND tipofac in (3032))
     OR t.tipo = "PHYSICAL")
     ),
    -->>>>>>COMPRAS EFECTUADA<<<<<<<
  compras_efectuada as (
 
  SELECT DISTINCT
    DATETIME(D.created, 'America/Santiago')  as timestamp_fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_ce,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '12.5. Compra efectuada' as paso,
    12.5 paso_num,
    u.uuid  ,
    us.source,
    FIRST_VALUE(m.impfac) 
      OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as monto,  
  FROM {{ source('prepago', 'prp_cuenta') }} c
  JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
  JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
  JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
  JOIN {{ ref('users_tenpo') }} us ON us.id = u.uuid
  JOIN {{ref('onboarding_ligth')}} A ON us.id = A.user
  JOIN {{ref('onboarding_ligth_created_at')}} D ON A.phone = D.phone_no_hash
  WHERE 
    tipofac in (3006,3007,3028,3029, 3031, 3009,5) 
  AND A.phone IS NOT NULL
    ),
   -->>>>>>COMPRAS EXITOSAS<<<<<<<
  compras_exitosas as (
 
  SELECT DISTINCT
    DATETIME(D.created, 'America/Santiago')  as timestamp_fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_ce,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '13. Compra exitosa' as paso,
    13 paso_num,
    u.uuid  ,
    us.source,
    FIRST_VALUE(m.impfac) 
      OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as monto,  
  FROM {{ source('prepago', 'prp_cuenta') }} c
  JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
  JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
  JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
  JOIN {{ ref('users_tenpo') }} us ON us.id = u.uuid
  JOIN {{ref('onboarding_ligth')}} A ON us.id = A.user
  JOIN {{ref('onboarding_ligth_created_at')}} D ON A.phone = D.phone_no_hash
  WHERE 
    tipofac in (3006,3007,3028,3029, 3031, 3009,5) 
    AND m.estado  in ('PROCESS_OK','AUTHORIZED','NOTIFIED')
     AND A.phone IS NOT NULL
  ),
  
    -->>>>>>FIRST CASHOUT EXITOSO<<<<<<<
  usuarios_co as (
  SELECT DISTINCT
    DATETIME(D.created, 'America/Santiago')  as timestamp_fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
        OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago")ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
        OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '14. First Cashout'  as paso,
    14 paso_num,
    u.uuid,
    us.source,
    FIRST_VALUE(m.impfac) 
        OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as monto,  
  FROM {{ source('prepago', 'prp_cuenta') }} c
  JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
  JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
  JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
  JOIN {{ ref('users_tenpo') }} us ON us.id = u.uuid
  JOIN {{ref('onboarding_ligth')}} A ON us.id = A.user
  JOIN {{ref('onboarding_ligth_created_at')}} D ON A.phone = D.phone_no_hash
  WHERE ((m.estado  = 'PROCESS_OK' AND tipofac =3004) OR (estado_de_negocio  in ('OK', 'CONFIRMED') AND tipofac = 3003))
  AND indnorcor  = 0 
  AND A.phone IS NOT NULL
  )

SELECT DISTINCT
  --DATE(created_at, "America/Santiago") as fecha_creacion,
  --t1.timestamp_fecha as fecha_creacion,
  --O.created as fecha_creacion,
  timestamp_fecha fecha_creacion,
  t1.timestamp_fecha, 
  t1.fecha as fecha, 
  t1.fecha_ob,
  FORMAT_DATE("%Y-%m-01", t1.fecha_ob) camada_ob,
  t1.fecha_funnel, 
  t1.paso as paso, 
  t1.paso_num,
  uuid,
  --/* cambio source */---
  t1.source,
  IF(source.media_source IS NULL, 'desconocido',source.media_source ) motor,
  --/* cambio source */---
  monto,
  grupo
FROM (
      SELECT
        *
      FROM usuarios_co
      
      UNION ALL
      
      SELECT
        *
      FROM usuarios_tarjetas 

      UNION ALL

      SELECT 
        * 
      FROM compras_exitosas 
      
      UNION ALL
      
      SELECT
        *
      FROM compras_efectuada 

      UNION ALL 

      SELECT 
        * 
      FROM usuarios_ci 

      UNION ALL 

      SELECT 
        * 
      FROM usuarios_inicio_ob_ligth

      UNION ALL 

      SELECT
        * 
      FROM usuarios_rut 

      UNION ALL 

      SELECT 
        * 
      FROM usuarios_ob_exitoso  

      UNION ALL

      SELECT 
        * 
      FROM ob_ligth_exitoso

      UNION ALL 

      SELECT 
        * 
      FROM guarda_correo

      UNION ALL

      SELECT 
        * 
      FROM valida_correo 

      UNION ALL 

      SELECT  
        * 
      FROM ingresa_direccion 

      UNION ALL 

      SELECT 
        * 
      FROM ingresa_ocupacion 

      UNION ALL 

      SELECT 
        *
      FROM guarda_telefono

      UNION ALL

      SELECT  
        * 
      FROM valida_telefono

      ) as t1

--LEFT JOIN {{ref('onboarding_ligth_created_at')}} O on O.user = uuid
LEFT JOIN {{ ref('appsflyer_users') }} source on source.customer_user_id = uuid 
LEFT JOIN (
  SELECT DISTINCT
    id as uuid, 
    created_at as created_at,   
    --/* cambio source */---
    --  WHEN source like '%invita y gana%' AND grupo is null THEN 'desconocido' 
    --  WHEN source not like '%invita y gana%'AND grupo is not null THEN null
    --/* cambio source */---
     CASE 
     WHEN source like '%IG_%' AND grupo is null THEN 'desconocido' 
     WHEN source not like '%IG_%'AND grupo is not null THEN null
     ELSE CAST(grupo AS STRING) END AS grupo,
  FROM {{ ref('users_tenpo') }}
  LEFT JOIN {{ ref('parejas_iyg') }} on user = id
  ) USING (uuid) 
WHERE 1=1
ORDER BY 
  1,2,3
