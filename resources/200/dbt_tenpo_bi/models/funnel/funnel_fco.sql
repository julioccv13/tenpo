{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  usuarios_fco as (
    SELECT DISTINCT
      fecha,
      user,
      monto,
      row_number() over (partition by user order by trx_timestamp asc) as row_num
    FROM {{ ref('economics') }}
    WHERE
      linea = 'cash_out'
      )

SELECT DISTINCT
  fecha fecha_fco,
  monto monto_fco,
  'First Cashout'  as paso,
  user
FROM usuarios_fco
WHERE 
    row_num = 1
