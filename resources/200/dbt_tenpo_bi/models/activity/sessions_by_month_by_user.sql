{{ 
  config(
    tags=["daily", "bi","sessions"],
    materialized='table', 
    cluster_by = "tenpo_uuid"
  ) 
}}

WITH  
  maus as (
    SELECT DISTINCT
      a.fecha 
      ,a.trx_timestamp 
      ,a.linea 
      ,a.trx_id 
      ,a.user 
      ,b.state
      ,b.email 
    FROM {{ ref('economics') }} a -- USING(user)
    JOIN {{ ref('users_tenpo') }} b on id = user --`tenpo-bi-prod.users.users_tenpo`
    WHERE true
      AND linea not in ('reward')
      AND nombre not like "%Devolución%"
       ),
  f_mau as (
    SELECT distinct
     user
     ,date_trunc(fecha, month) mes_mau
     ,MAX(case when trx_id is not null then true else false end) f_mau
    FROM maus
    GROUP BY
      1,2
      ),
  visitas_clevertap as (
       SELECT DISTINCT 
         fecha
         ,mes_visita 
         ,tenpo_uuid 
         ,session_id 
         ,session_lenght 
       FROM {{ ref('valid_sessions') }}  --`tenpo-bi-prod.activity.valid_sessions` 
       
        ),
  resumen_visitas_mes as (
      SELECT
        mes_visita,
        a.tenpo_uuid,
        COALESCE(f_mau, false) f_mau,
        COUNT(DISTINCT a.session_id ) visitas_totales,
        ROUND(AVG(session_lenght),3) duracion_promedio_sesion,
        SUM(session_lenght) duracion_total_sesion,
      FROM visitas_clevertap a
      LEFT JOIN f_mau b on  a.tenpo_uuid = b.user and a.mes_visita = b.mes_mau
      GROUP BY
        1,2,3
        )
    SELECT 
      *
    FROM resumen_visitas_mes 
    WHERE TRUE
    --AND f_mau is true
--     AND tenpo_uuid = "f4f0f178-4e9a-45b0-b69c-8c768b56c1fe" 
