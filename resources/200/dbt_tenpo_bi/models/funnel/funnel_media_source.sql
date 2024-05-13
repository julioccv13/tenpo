{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  pre_funnel_motor as (
        WITH
          datos_media_source as (
              SELECT DISTINCT 
                user
                ,first_event_time
                ,last_media_source 
              FROM {{ ref('appsflyer_users_all_states') }} --`tenpo-bi-prod.external.appsflyer_users_all_states` 
              ),
          economics as (
            SELECT
              fecha
              ,user
              ,case when linea in ('cash_in', 'paypal', 'p2p_received') then 'cash_in' else linea end as linea
              ,row_number() OVER (PARTITION BY user,  case when linea in ('cash_in', 'paypal', 'p2p_received') then 'cash_in' else linea end  ORDER BY fecha ) row_num
            FROM {{ ref('economics') }} --{{ref('economics')}} 
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
          ,last_media_source
          ,DATE( first_event_time , "America/Santiago") fecha_primer_evento_install
        FROM  datos_media_source 
        LEFT JOIN {{ ref('users_tenpo') }} a ON id = user
        LEFT JOIN (SELECT user, fecha FROM economics WHERE linea = 'mastercard' AND row_num = 1) compra ON compra.user = a.id 
        LEFT JOIN (SELECT user, fecha FROM economics WHERE linea = 'cash_in' AND row_num = 1) cashin ON cashin.user = a.id 
--         WHERE a.id = 'e5cdfa0e-9d90-49bc-96b5-06534c52c855'
    ),
    -->>>>> APP INSTALLS <<<<<<< 
  descargas as(
     SELECT DISTINCT
       fecha_primer_evento_install fecha ,
       CAST(null as DATE) fecha_ob,
       'Instala app'  as paso,
       1 paso_num,
       user,  
       last_media_source
     FROM pre_funnel_motor 
       ),
    -->>>>> INICIO ONBOARDING <<<<<<< 
  usuarios_inicio_ob as(
     SELECT DISTINCT
       created_at fecha,
       fecha_ob,
       'Inicio OB'  as paso,
       2 paso_num,
       user,
       last_media_source
     FROM
      pre_funnel_motor u
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
       last_media_source
     FROM
      pre_funnel_motor u
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
       last_media_source
     FROM
      pre_funnel_motor u
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
       last_media_source
      FROM
      pre_funnel_motor u
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
        last_media_source
       FROM
        pre_funnel_motor u
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
        last_media_source
       FROM
        pre_funnel_motor u
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
        last_media_source
       FROM
        pre_funnel_motor u
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