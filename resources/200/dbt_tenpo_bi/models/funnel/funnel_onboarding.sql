{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  usuarios_inicio_ob as(
    SELECT DISTINCT
      DATETIME(created_at , 'America/Santiago')  as fecha,
      id,
    FROM {{ ref('users_tenpo') }}  
    WHERE
      state in (0,1,2,3,4,6,9,8,7,21,22,16)
  ), 
  usuarios_ob_exitoso as (
    SELECT
      DATETIME(ob_completed_at , 'America/Santiago')  as fecha,
      id,
    FROM {{ ref('users_tenpo') }} 
    WHERE
      state in (4,8,7,21,22) 
      AND ob_completed_at is not null
      ),
  union_tablas as (
    SELECT 
      id user,
      DATE_DIFF(CAST(o.fecha AS DATE), CAST(i.fecha AS DATE), DAY) dias_diff,
      CAST(i.fecha AS DATE) fecha_inicio_ob,
      CAST(o.fecha AS DATE) fecha_ob,
      DATE_ADD(CAST(o.fecha AS DATE), INTERVAL -1 DAY) dia_anterior,
      DATE_ADD(CAST(o.fecha AS DATE), INTERVAL -1 WEEK) semana_anterior,
      DATE_ADD(CAST(o.fecha AS DATE), INTERVAL -1 MONTH) mes_anterior,
    FROM usuarios_inicio_ob i
    JOIN usuarios_ob_exitoso o USING( id ))
 
SELECT DISTINCT
  *,
  IF(DATE_DIFF(dia_anterior, fecha_inicio_ob, DAY) < 0, 'mismo dia' , IF(DATE_DIFF(dia_anterior, fecha_inicio_ob, DAY) = 0 , 'dia anterior' , 'otro')) dia_inicia_ob,
  IF(DATE_DIFF(semana_anterior, fecha_inicio_ob, DAY) <= 0, 'misma semana' , IF(DATE_DIFF(semana_anterior, fecha_inicio_ob, DAY) > 0 AND DATE_DIFF(semana_anterior, fecha_inicio_ob, DAY) <= 7 , 'semana anterior' , 'otro')) semana_inicio_ob,
  IF(DATE_DIFF(mes_anterior, fecha_inicio_ob, DAY) <= 0, 'mismo mes' , IF(DATE_DIFF(mes_anterior, fecha_inicio_ob, DAY) > 30 AND DATE_DIFF(mes_anterior, fecha_inicio_ob, DAY) <= 60 , 'mes anterior' , 'otro')) mes_inicio_ob,
FROM union_tablas



