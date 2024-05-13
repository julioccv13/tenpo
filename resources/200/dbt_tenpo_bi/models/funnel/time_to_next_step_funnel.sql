
{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}



WITH 
  inicio_ob as (
    SELECT DISTINCT
      uuid user,
      timestamp_fecha fecha_io
    FROM {{ ref('funnel_tenpo')}} 
    WHERE paso = '1. Inicio OB'
    ),
  ob as (
    SELECT DISTINCT
      uuid user,
      timestamp_fecha fecha_ob
    FROM {{ ref('funnel_tenpo')}} 
    WHERE paso = '7. OB exitoso'),
  fci as (
    SELECT DISTINCT
      uuid user,
      timestamp_fecha fecha_fci
    FROM {{ ref('funnel_tenpo')}} 
    WHERE paso = '8. First Cashin')
  
 SELECT DISTINCT
    user,
    CAST(fecha_io as DATE) fecha_io,
    CAST(fecha_ob as DATE) fecha_ob,
    CAST(fecha_fci as DATE) fecha_fci,
    TIMESTAMP_DIFF(fecha_ob,fecha_io, MINUTE ) min_to_ob,
    TIMESTAMP_DIFF(fecha_fci,fecha_ob, MINUTE ) min_to_fci
  FROM inicio_ob
  LEFT JOIN ob USING(user)
  LEFT JOIN fci USING(user)
  
  