{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  usuarios_fci as (
    SELECT DISTINCT
      fecha,
      user,
      monto,
      row_number() over (partition by user order by trx_timestamp asc) as row_num
    FROM {{ ref('economics') }}
    WHERE
      linea = 'cash_in'
       )

SELECT DISTINCT
  fecha fecha_fci,
  monto monto_fci,
  'First Cashin'  as paso,
  user
FROM usuarios_fci
WHERE 
  row_num = 1
