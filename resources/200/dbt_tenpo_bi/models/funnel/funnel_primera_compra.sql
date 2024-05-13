{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  usuarios_primera_compra as (
    SELECT DISTINCT
      fecha,
      user,
      monto,
      row_number() over (partition by user order by trx_timestamp asc) as row_num
    FROM {{ ref('economics') }}
    WHERE
      linea = 'mastercard'
       )

SELECT DISTINCT
  fecha fecha_ce,
  monto monto_ce,
  'Compra exitosa'  as paso,
  user
FROM usuarios_primera_compra
WHERE 
  row_num = 1