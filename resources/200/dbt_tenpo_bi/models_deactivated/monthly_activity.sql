
{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "user",
    partition_by = {'field': 'mes', 'data_type': 'date'},
  ) 
}}

WITH 
  rubros_unicos as (
    SELECT
      FORMAT_DATE("%Y-%m-01", Fecha_Fin_Analisis_DT) mes,
      tenpo_uuid user,
      active_buyer_mc_app ,
      countuniq_rubro_act_general_30_dias,
      churn,
      FORMAT_DATE("%Y-%m-01", DATE(ob_completed_at , "America/Santiago")) camada,
      CASE WHEN tenpo_user_persona is null THEN 'unknown' ELSE tenpo_user_persona END AS tenpo_user_persona,
      countuniq_rubro_act_general_origen
    FROM
      {{source('tablones_analisis','tablon_monthly_vectores_usuarios')}}
    WHERE
      tenpo_uuid IS NOT NULL
      AND state in (4,7,8,21,22)
      AND ob_completed_at is not null
      ),
   visitas_clevertap as (
       SELECT DISTINCT 
         PARSE_DATE("%Y%m%d", CAST(date as STRING)) date,
         id user,
         session_id,
         session_lenght,
         ROW_NUMBER() OVER (PARTITION BY  email, session_id , date ORDER BY fecha_hora ) ro_n
       FROM {{source('clevertap','events')}}
       JOIN {{ref('users_tenpo')}}  USING(email)
       WHERE
         event = 'Session Concluded'
         AND state in (4,7,8,21,22)
         AND ob_completed_at is not null
    ),
    visitas_unicas as (
     SELECT 
        * EXCEPT(ro_n)
     FROM visitas_clevertap
     WHERE ro_n = 1
    ),
    calculos_visitas as (
    SELECT
      FORMAT_DATE("%Y-%m-01", date) mes,
      user,
      COUNT(distinct session_id) visitas,
      AVG(session_lenght) duracion_promedio_sesion,
    FROM visitas_unicas 
    WHERE TRUE
    AND session_lenght > 0
    AND session_lenght <= 1200
    GROUP BY 1,2
 )
    
    
    SELECT DISTINCT
      CAST(mes as DATE) mes,
      user,
      churn,
      camada,
      active_buyer_mc_app ,
      tenpo_user_persona,
      countuniq_rubro_act_general_30_dias rubros_unicos_30_dias,
      countuniq_rubro_act_general_origen rubros_unicos_origen,     
      visitas visitas_mes,
      duracion_promedio_sesion duracion_promedio_sesion_mes 
    FROM rubros_unicos
    LEFT JOIN calculos_visitas USING(user, mes)