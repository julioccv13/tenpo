{{ config(materialized='view',  tags=["daily", "bi"]) }}
SELECT 
  *,
  "PayPal" as linea,
FROM {{source('aux_paypal','aux_campanas')}}
UNION ALL
SELECT 
  *,
  "TopUps" as linea
from {{source('aux_topups','aux_campanas')}}
UNION ALL
SELECT 
  {{ hash_sensible_data('email') }} as email,
  nombre_campana,
  date(fecha_inicio) fecha_inicio,
  date(fecha_fin) fecha_fin,
  linea STRING
from {{source('aux_table','bases_campanas_bucket')}}