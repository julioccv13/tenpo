{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH 
 -->>>>> INICIO ONBOARDING <<<<<<< 
 usuarios_inicio_ob as(
  SELECT DISTINCT
    DATETIME(created_at , 'America/Santiago')  as timestamp_fecha,
    DATE(created_at , 'America/Santiago')  as fecha,
    DATE(created_at , 'America/Santiago')  as fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '1. Inicio OB'  as paso,
    1 paso_num,
    id,
    u.source ,
    0 monto
  FROM
    {{ ref('users_tenpo') }} u
  WHERE
    state in (0,1,2,3,4,6,9,8,7,21,22,16)
    ),
 -->>>>> VALIDA CORREO <<<<<<< 
 valida_correo as(
  SELECT DISTINCT 
    DATETIME(created_at , 'America/Santiago')  as timestamp_fecha,
    DATE(created_at , 'America/Santiago') as fecha,
    DATE(created_at , 'America/Santiago')  as fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '2. Valida correo'  as paso,
    2 paso_num,
    id,
    u.source,
    0 monto
  FROM
    {{ ref('users_tenpo') }} u
  WHERE
    state in (1,2,3,4,6,9,8,7,21,22)
    ),
  -->>>>> INGRESA RUT CORREO <<<<<<< 
  usuarios_rut as (
  SELECT DISTINCT
    DATETIME(created_at , 'America/Santiago')  as timestamp_fecha,
    DATE(created_at , 'America/Santiago')  as fecha,
    DATE(created_at , 'America/Santiago')  as fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '3. Ingresa RUT'  as paso,
    3 paso_num,
    id,
    u.source,
    0 monto
  FROM
    {{ ref('users_tenpo') }} u
  WHERE
    state in (1,2,3,4,6,8,7,21,22)
    ),
  -->>>>>INGRESA OCUPACIÓN<<<<<<< 
  ingresa_ocupacion as (
   SELECT
      DATETIME(created_at , 'America/Santiago')  as timestamp_fecha,
      DATE(created_at , 'America/Santiago')  as fecha,
      DATE(created_at , 'America/Santiago')  as fecha_funnel,
      DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
      '4. Ingresa ocupación' as paso,
      4 paso_num,
      id,
      u.source,
      0 monto
   FROM
    {{ ref('users_tenpo') }} u
   WHERE
    (state in (2,3,4,8,7,21,22) 
    AND  profession is not null) 
     ),
  -->>>>> VALIDA CELULAR <<<<<<< 
  valida_celular as(
   SELECT
    DATETIME(created_at , 'America/Santiago')  as timestamp_fecha,
    DATE(created_at , 'America/Santiago')  as fecha,
    DATE(created_at , 'America/Santiago')  as fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '5. Valida celular'  as paso,
    5 paso_num,
    id,
    u.source,
    0 monto
   FROM
    {{ ref('users_tenpo') }} u
   WHERE
    (state in (3,4,8,7,21,22) 
    AND  profession is not null)  
   ),
  -->>>>>INGRESA DIRECCIÓN <<<<<<< 
  ingresa_direccion as(
    SELECT
      DATETIME(created_at , 'America/Santiago')  as timestamp_fecha,
      DATE(created_at , 'America/Santiago')  as fecha,
      DATE(created_at , 'America/Santiago')  as fecha_funnel,
      DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
      '6. Ingresa direccion'  as paso,
      6 paso_num,
      id,
      u.source,
      0 monto
    FROM
      {{ ref('users_tenpo') }} u
    WHERE
      (state in (3,4,8,7,21,22)  
      AND region_code is not null)
      ), 
  -->>>>>ONBOARDING EXITOSO<<<<<<< 
  usuarios_ob_exitoso as (
    SELECT
      DATETIME(ob_completed_at , 'America/Santiago')  as timestamp_fecha,
      DATE(created_at , 'America/Santiago')  as fecha,
      DATE(ob_completed_at , 'America/Santiago')  as fecha_funnel,
      DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
      '7. OB exitoso'  as paso,
      7 paso_num,
      id,
      u.source,
      0 monto
    FROM
      {{ ref('users_tenpo') }} u
    WHERE
      state in (4,8,7,21,22) 
      AND ob_completed_at is not null
      ),
 -->>>>>FIRST CASHIN <<<<<<< 
  usuarios_ci as (
    SELECT DISTINCT
      FIRST_VALUE(DATETIME(m.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATETIME(m.fecha_creacion , "America/Santiago")ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as timestamp_fecha,
      FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago")ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha,
      FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
      DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
      '8. First Cashin'  as paso,
      8 paso_num,
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
    WHERE
      m.estado    = 'PROCESS_OK' 
      AND indnorcor  = 0 
      AND tipofac in (3001,3002)
    ),
  -->>>>>TARJETA ACTIVA <<<<<<< 
  usuarios_tarjetas as (
  
  SELECT DISTINCT
    FIRST_VALUE(DATETIME(t.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATETIME(t.fecha_creacion , "America/Santiago")ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as timestamp_fecha,
    FIRST_VALUE(DATE(t.fecha_creacion ,  "America/Santiago" ) ) OVER (PARTITION BY u.uuid ORDER BY DATE(t.fecha_creacion ,  "America/Santiago" ) ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha, 
    FIRST_VALUE(DATE(t.fecha_creacion, "America/Santiago"))
      OVER (PARTITION BY u.uuid ORDER BY DATE(t.fecha_creacion ,  "America/Santiago" ) ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '9. Activa tarjeta' as paso,
    9 paso_num,
    u.uuid,
    us.source,
    0 monto
  FROM {{ source('prepago', 'prp_cuenta') }} c
  JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
  JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
  LEFT JOIN {{ source('prepago', 'prp_movimiento') }} m ON m.id_tarjeta = t.id
  JOIN {{ ref('users_tenpo') }} us ON us.id = u.uuid
  WHERE 
    TRUE 
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
    FIRST_VALUE(DATETIME(m.fecha_creacion , "America/Santiago")) 
        OVER (PARTITION By u.uuid ORDER BY DATETIME(m.fecha_creacion , "America/Santiago")ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as timestamp_fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_ce,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '9.5. Compra efectuada' as paso,
    9.5 paso_num,
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
  WHERE 
    tipofac in (3006,3007,3028,3029, 3031, 3009,5) 
    ),
   -->>>>>>COMPRAS EXITOSAS<<<<<<<
  compras_exitosas as (
 
  SELECT DISTINCT
    FIRST_VALUE(DATETIME(m.fecha_creacion , "America/Santiago")) 
        OVER (PARTITION By u.uuid ORDER BY DATETIME(m.fecha_creacion , "America/Santiago")ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as timestamp_fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_ce,
    FIRST_VALUE(DATE(m.fecha_creacion , 'America/Santiago'))
      OVER (PARTITION BY u.uuid ORDER BY DATE(m.fecha_creacion , 'America/Santiago') ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '10. Compra exitosa' as paso,
    10 paso_num,
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
  WHERE 
    tipofac in (3006,3007,3028,3029, 3031, 3009,5) 
    AND m.estado  in ('PROCESS_OK','AUTHORIZED','NOTIFIED')
  ),
  
    -->>>>>>FIRST CASHOUT EXITOSO<<<<<<<
  usuarios_co as (
  SELECT DISTINCT
    FIRST_VALUE(DATETIME(m.fecha_creacion , "America/Santiago")) 
          OVER (PARTITION By u.uuid ORDER BY DATETIME(m.fecha_creacion , "America/Santiago")ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as timestamp_fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
        OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago")ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha,
    FIRST_VALUE(DATE(m.fecha_creacion , "America/Santiago")) 
        OVER (PARTITION By u.uuid ORDER BY DATE(m.fecha_creacion , "America/Santiago") ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
    DATE(ob_completed_at , 'America/Santiago')  as fecha_ob,
    '11. First Cashout'  as paso,
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
  JOIN {{ ref('users_tenpo') }} us ON us.id = u.uuid
  WHERE 
      ((m.estado  = 'PROCESS_OK' AND tipofac =3004) OR (estado_de_negocio  in ('OK', 'CONFIRMED') AND tipofac = 3003))
      AND indnorcor  = 0 
      )
  
SELECT DISTINCT
  DATE(created_at, "America/Santiago") as fecha_creacion,
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
      FROM usuarios_inicio_ob 

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
      FROM valida_celular 

      ) as t1

LEFT JOIN {{ ref('appsflyer_users') }} source on source.customer_user_id = uuid 
JOIN (
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

--LEFT JOIN {{ ref('users_onboarding_ligth') }} ob_ligth ON ob_ligth.user = uuid  WHERE ob_ligth.user IS NULL
LEFT JOIN {{source('tenpo_users','onboarding_PROD')}} ob_ligth ON ob_ligth.user_id = uuid  WHERE ob_ligth.user_id IS NULL
--WHERE 1=1
ORDER BY 
  1,2,3


