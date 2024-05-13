{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}


WITH 
    pre_funnel_dispositivos as (
        WITH
          datos as (
              SELECT DISTINCT 
                identity user
                ,objectId 
                ,model device_type
                ,make device_brand
                ,app_version
                ,plataform  platform
                ,fecha_hora event_time
              FROM {{source('clevertap','events_app_install')}}

            ),
          economics as (
            SELECT
              fecha
              ,user
              ,case when linea in ('cash_in', 'paypal', 'p2p_received') then 'cash_in' else linea end as linea
              ,row_number() OVER (PARTITION BY user,  case when linea in ('cash_in', 'paypal', 'p2p_received') then 'cash_in' else linea end  ORDER BY fecha ) row_num
            FROM {{ref('economics')}} 
            WHERE 
              linea in ('mastercard','cash_in', 'paypal', 'p2p_received')
              )

        SELECT DISTINCT
          a.id user
          ,a.state
          ,a.profession
          ,a.region_code
          ,DATE(a.created_at, "America/Santiago") created_at
          ,DATE(a.ob_completed_at, "America/Santiago") fecha_ob
          ,cashin.fecha as fecha_fci
          ,compra.fecha fecha_compra
          ,LAST_VALUE(device_type) OVER (PARTITION BY datos.user  ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )  last_device_type
          ,LAST_VALUE(app_version) OVER (PARTITION BY datos.user  ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )  last_version
          ,LAST_VALUE(device_brand) OVER (PARTITION BY datos.user  ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_brand
          ,LAST_VALUE(platform) OVER (PARTITION BY datos.user  ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_platform
          ,LAST_VALUE(event_time) OVER (PARTITION BY datos.user  ORDER BY event_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING )   last_event_time
        FROM  {{ref('users_tenpo')}} a
        JOIN datos ON user = id AND DATE(event_time, "America/Santiago") <= DATE(created_at, "America/Santiago") 
        LEFT JOIN (SELECT user, fecha FROM economics WHERE linea = 'mastercard' AND row_num = 1) compra ON compra.user = a.id 
        LEFT JOIN (SELECT user, fecha FROM economics WHERE linea = 'cash_in' AND row_num = 1) cashin ON cashin.user = a.id 
        --WHERE user = '1827c757-388c-4698-8af0-11a515f6b7e2'
        ),
    -->>>>> APP INSTALLS <<<<<<< 
    descargas as(
     SELECT DISTINCT
       DATE(fecha_hora, "America/Santiago") fecha,
       CAST(null as DATE) fecha_ob,
       'Instala app'  as paso,
       1 paso_num,
       objectId user,
       LAST_VALUE(app_version) OVER (PARTITION BY objectId  ORDER BY fecha_hora ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_version, 
       LAST_VALUE(make) OVER (PARTITION BY objectId  ORDER BY fecha_hora ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_brand, 
       LAST_VALUE(plataform) OVER (PARTITION BY objectId  ORDER BY fecha_hora ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_platform,
       LAST_VALUE(model) OVER (PARTITION BY objectId  ORDER BY fecha_hora ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) last_device_type
     FROM
      {{source('clevertap','events_app_install')}} 
       
       ),
    -->>>>> INICIO ONBOARDING <<<<<<< 
    usuarios_inicio_ob as(
     SELECT DISTINCT
       created_at fecha,
       fecha_ob,
       'Inicio OB'  as paso,
       2 paso_num,
       user,
       last_version, 
       last_device_brand, 
       last_platform,
       last_device_type
     FROM
      pre_funnel_dispositivos u
     WHERE
       state in (0,1,2,3,4,6,9,8,7,16)
       ),
    -->>>>> VALIDA CORREO <<<<<<< 
    valida_correo as(
     SELECT DISTINCT 
       created_at fecha,
       fecha_ob,
       'Valida correo'  as paso,
       3 paso_num,
       user,
       last_version, 
       last_device_brand, 
       last_platform,
       last_device_type
     FROM
      pre_funnel_dispositivos u
     WHERE
       state in (1,2,3,4,6,9,8,7)
       ),
    -->>>>> INGRESA RUT <<<<<<< 
    ingresa_rut as(
     SELECT DISTINCT 
       created_at fecha,
       fecha_ob,
       'Ingresa RUT'  as paso,
       4 paso_num,
       user,
       last_version, 
       last_device_brand, 
       last_platform,
       last_device_type
     FROM
      pre_funnel_dispositivos u
     WHERE
       state in (1,2,3,4,6,8,7)
       ),
     -->>>>> VALIDA CELULAR <<<<<<< 
    valida_celular as(
      SELECT
       created_at fecha,
       fecha_ob,
       'Valida celular'  as paso,
       5 paso_num,
       user,
       last_version, 
       last_device_brand, 
       last_platform,
       last_device_type
      FROM
      pre_funnel_dispositivos u
      WHERE
       (state in (3,4,8,7) AND  profession is not null)  
      ),
     -->>>>>ONBOARDING EXITOSO<<<<<<< 
    usuarios_ob_exitoso as (
       SELECT
        created_at fecha,
        fecha_ob,
        'OB exitoso'  as paso,
        6 paso_num,
        user,
        last_version, 
        last_device_brand, 
        last_platform,
        last_device_type
       FROM
        pre_funnel_dispositivos u
       WHERE
         state in (4,8,7) 
         AND fecha_ob is not null
         ),
    usuarios_fci as (
       SELECT
        fecha_fci fecha,
        fecha_ob,
        'First Cash-in'  as paso,
        7 paso_num,
        user,
        last_version, 
        last_device_brand, 
        last_platform,
        last_device_type
       FROM
        pre_funnel_dispositivos u
       WHERE
         state in (4,8,7) 
         AND fecha_ob is not null
         AND fecha_fci is not null
         ),
    usuarios_compra as (
       SELECT
        fecha_compra fecha,
        fecha_ob,
        'Compra exitosa'  as paso,
        8 paso_num,
        user,
        last_version, 
        last_device_brand, 
        last_platform,
        last_device_type
       FROM
        pre_funnel_dispositivos u
       WHERE
         state in (4,8,7) 
         AND fecha_ob is not null
         AND fecha_compra is not null
         )


SELECT * FROM descargas  
UNION ALL          
SELECT * FROM usuarios_inicio_ob  
UNION ALL 
SELECT * FROM valida_correo  
UNION ALL 
SELECT * FROM ingresa_rut  
UNION ALL 
SELECT * FROM valida_celular 
UNION ALL 
SELECT *  FROM usuarios_ob_exitoso  
UNION ALL 
SELECT *  FROM usuarios_fci  
UNION ALL 
SELECT *  FROM usuarios_compra