
{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table'
  ) 
}}
WITH

log_aportes_rescates as ( 
    -------------------------------------
    --           APORTES               --
    -------------------------------------
    SELECT DISTINCT
      cash_in_id trx_id,
      DATETIME(ci.created_date  , "America/Santiago") creacion,
      user_id  user,
      amount,
      status,
      'aporte' tipo,
      origin
    FROM  {{ source('payment_savings', 'cash_in') }}   ci
     JOIN {{ ref('users_savings') }} on user_id = id
    WHERE 
      ci.status in ( 'SUCCESSFUL', 'PENDING', 'CONFIRMED', 'CREATED') 
      AND register_status in ('FINISHED')
      AND onboarding_status in ('FINISHED')
      
      UNION ALL
     
    -------------------------------------
    --           RESCATES              --
    -------------------------------------
     SELECT DISTINCT
      co.cash_out_id  trx_id,
      DATETIME(co.created_date  , "America/Santiago") creacion,
      user_id  user,
      amount,
      status,
      'rescate' tipo,
      origin
    FROM  {{ source('payment_savings', 'cash_out') }}  co
     JOIN {{ ref('users_savings') }} on user_id = id
    WHERE 
      status in ( 'SUCCESSFUL', 'PENDING', 'CONFIRMED', 'CREATED', 'PENDING_SETTLEMENT')
      AND register_status in ('FINISHED')
      AND onboarding_status in ('FINISHED')
    )

  SELECT
    * 
  FROM(
    SELECT  
      user, 
      tipo,
      creacion,
      IF(tipo = 'rescate' AND LAG(tipo) OVER (PARTITION BY user ORDER BY creacion) = 'aporte' ,
      DATETIME_DIFF(creacion, LAG(creacion) OVER (PARTITION BY user ORDER BY creacion) ,HOUR) , null) dif_tiempo,
      origin
    FROM log_aportes_rescates  
    ) 
    WHERE 
      dif_tiempo is not null
