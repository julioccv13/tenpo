{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}


WITH 
  cashin as (
    SELECT 
      u.tributary_identifier RUT,
      first_name NOMBRES,
      last_name APELLIDOS,
      participant_id ID_PARTICIPE, 
      operation_id ID_OPERACION,
      DATETIME(ci.confirmed_date,"America/Santiago") FECHA_INGRESO,
      CAST(null AS DATE) FECHA_PAGO, 
      fee_quantity CUOTAS,
      ci.fee_value VALOR,
      ci.amount MONTO,
      status,
      confirmed_date, pending_date, rejected_date, last_retry_date, successful_date, balance_rejected_date,
      'CASH-IN' TIPO_OPERACION,
      origin
    FROM {{ source('payment_savings', 'cash_in') }} ci
    JOIN {{ ref('users_savings') }} u ON user_id = id),
    
  cashout as (
    SELECT 
      u.tributary_identifier RUT,
      first_name NOMBRES,
      last_name APELLIDOS,
      co.participant_id  ID_PARTICIPE, 
      co.operation_id  ID_OPERACION,
      DATETIME(co.confirmed_date ,"America/Santiago")  FECHA_INGRESO,
      CAST(co.settlement_date AS DATE)  FECHA_PAGO, 
      fee_quantity CUOTAS,
      co.fee_value VALOR,
      co.amount MONTO,
      status,
      confirmed_date, pending_date, rejected_date, last_retry_date, successful_date, balance_rejected_date,
      'CASH-OUT' TIPO_OPERACION,
      origin
    FROM {{ source('payment_savings', 'cash_out') }} co
    JOIN {{ ref('users_savings') }} u ON user_id = id 
  
  )
  
  
  SELECT * FROM cashin 
  UNION ALL 
  SELECT * FROM cashout