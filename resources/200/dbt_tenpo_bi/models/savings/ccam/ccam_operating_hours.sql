{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}

WITH
  cashins as (
    SELECT DISTINCT
      u.tributary_identifier  rut,
      u.first_name name,
      u.last_name last_name,
      DATETIME(pending_date, "America/Santiago") pending_date,
      CAST(DATETIME(pending_date, "America/Santiago") AS TIME) pending_time,
      DATETIME(created_date, "America/Santiago") created_date,
      CASE 
       WHEN EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (7,1) OR (EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (2,3,4,5,6) 
        AND TIME(DATETIME(created_date, "America/Santiago")) NOT BETWEEN  '09:00:00' AND '12:45:00' ) 
        OR (DATE(created_date, "America/Santiago") IN (SELECT DISTINCT DATE(date, "America/Santiago") FROM {{ source('payment_savings', 'holidays') }})) THEN 'Fuera horario funcionamiento fondo' 
       # Operaciones cuya fecha y hora de pending_date correspondan a días no hábiles (feriados o fines de semana) o en horario no hábil en días hábiles --> cash-in: después de las 1245 pm y antes de las 9 am
       ELSE  'Dentro horario funcionamiento fondo'
       END AS pending_reason,      
      'aporte' request_type,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS DATE) confirmed_date,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS TIME) confirmed_time,
      origin
    FROM {{ source('payment_savings', 'cash_in') }} ci 
    JOIN {{ ref('users_savings') }} u ON u.id = ci.user_id
    WHERE 
      ci.status  = 'SUCCESSFUL'),
      
  cashin_no_invertido as (
    SELECT DISTINCT
      u.tributary_identifier  rut,
      u.first_name name,
      u.last_name last_name,
      DATETIME(pending_date, "America/Santiago") pending_date,
      CAST(DATETIME(pending_date, "America/Santiago") AS TIME) pending_time,
      DATETIME(created_date, "America/Santiago") created_date,
      CASE 
       WHEN EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (7,1) OR (EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (2,3,4,5,6) 
        AND  TIME(DATETIME(created_date, "America/Santiago")) NOT BETWEEN  '09:00:00' AND '12:45:00' )
        OR (DATE(created_date, "America/Santiago") IN (SELECT DISTINCT DATE(date, "America/Santiago") FROM {{ source('payment_savings', 'holidays') }})) THEN 'Fuera horario funcionamiento fondo' 
       # Operaciones cuya fecha y hora de pending_date correspondan a días no hábiles (feriados o fines de semana) o en horario no hábil en días hábiles --> cash-in: después de las 1245 pm y antes de las 9 am
       ELSE  'Dentro horario funcionamiento fondo'
       END AS pending_reason,      
      'aporte no invertido' request_type,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS DATE) confirmed_date,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS TIME) confirmed_time,   
      origin
    FROM {{ source('payment_savings', 'cash_in') }} ci
    JOIN {{ ref('users_savings') }} u ON u.id = ci.user_id
    WHERE ci.status  in ( 'CREATED', 'PENDING', 'CONFIRMED', 'REJECTED')
    ),
    
  cashouts as (
    SELECT DISTINCT
      u.tributary_identifier rut,
      u.first_name name,
      u.last_name last_name,
      DATETIME(pending_date, "America/Santiago") pending_date,
      CAST(DATETIME(pending_date, "America/Santiago") AS TIME) pending_time,
      DATETIME(created_date, "America/Santiago") created_date,
      CASE 
       WHEN EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago")) IN (7,1)  OR (EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (2,3,4,5,6)
        AND TIME(DATETIME(created_date, "America/Santiago"))  NOT BETWEEN '09:00:00' AND '14:00:00' )
        OR (DATE(created_date, "America/Santiago") IN (SELECT DISTINCT DATE(date, "America/Santiago") FROM {{ source('payment_savings', 'holidays') }})) THEN 'Fuera horario funcionamiento fondo' 
       # Operaciones cuya fecha y hora de pending_date correspondan a días no hábiles (feriados o fines de semana) o en horario no hábil en días hábiles--> cash-out: después de las 14 pm y antes de las 9 am
       ELSE  'Dentro horario funcionamiento fondo'
       END AS pending_reason,    
      'rescate' request_type,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS DATE) confirmed_date,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS TIME) confirmed_time,
      origin
    FROM {{ source('payment_savings', 'cash_out') }} co
    JOIN {{ ref('users_savings') }} u ON u.id = co.user_id
    WHERE 
      co.status  in ( 'SUCCESSFUL', 'PENDING_SETTLEMENT')
     
    ),
    
  cashout_no_invertido as (
    SELECT DISTINCT
      u.tributary_identifier rut,
      u.first_name name,
      u.last_name last_name,
      DATETIME(pending_date, "America/Santiago") pending_date,
      CAST(DATETIME(pending_date, "America/Santiago") AS TIME) pending_time,
      DATETIME(created_date, "America/Santiago") created_date,
      #IF(TIME(DATETIME(created_date, "America/Santiago")) NOT BETWEEN '09:00:00' AND '14:00:00' ,1,0),
      CASE 
       WHEN EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (7,1) OR (EXTRACT( DAYOFWEEK FROM DATETIME(created_date, "America/Santiago") ) IN (2,3,4,5,6)
        AND TIME(DATETIME(created_date, "America/Santiago")) NOT BETWEEN '09:00:00' AND '14:00:00' ) 
        OR ((DATE(created_date, "America/Santiago") IN (SELECT DISTINCT DATE(date, "America/Santiago")FROM {{ source('payment_savings', 'holidays') }}))) THEN 'Fuera horario funcionamiento fondo' 
       # Operaciones cuya fecha y hora de pending_date correspondan a días no hábiles (feriados o fines de semana) o en horario no hábil en días hábiles--> cash-out: después de las 14 pm y antes de las 9 am
       ELSE 'Dentro horario funcionamiento fondo'
       END AS pending_reason,    
      'rescate no invertido' request_type,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS DATE) confirmed_date,
      CAST(DATETIME(confirmed_date, "America/Santiago") AS TIME) confirmed_time,
      origin
    FROM {{ source('payment_savings', 'cash_out') }} co --{{ source('payment_savings', 'cash_out') }} co
    JOIN {{ ref('users_savings') }} u ON u.id = co.user_id
    WHERE 
      co.status  in ( 'CREATED', 'PENDING', 'CONFIRMED', 'REJECTED') 
   
    )
    
    
    SELECT * FROM cashins UNION ALL
    SELECT * FROM cashouts UNION ALL
    SELECT * FROM cashin_no_invertido UNION ALL
    SELECT * FROM cashout_no_invertido 