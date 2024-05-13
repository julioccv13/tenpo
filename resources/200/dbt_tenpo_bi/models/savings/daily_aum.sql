
{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    partition_by = {'field': 'Fecha_Analisis', 'data_type': 'date'},
  ) 
}}
   SELECT DISTINCT 
    *
   FROM {{ source('bolsillo', 'daily_aum') }}  