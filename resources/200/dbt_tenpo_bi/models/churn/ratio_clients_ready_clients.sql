{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}



WITH 
  onboardings AS (
    SELECT 
      id as user
      ,extract(year from ob_completed_at)*12 + extract(month from ob_completed_at) as mes_camada
      ,date_trunc(ob_completed_at, month) as dt_mes_camada
    FROM {{ ref('users_tenpo') }} --`tenpo-bi-prod.users.users_tenpo`
    where ob_completed_at is not null
), churn_vectors AS (
    SELECT
      user
      ,extract(year from periodo_legible)*12 + extract(month from periodo_legible) as mes_analisis
      ,date_trunc(periodo_legible, month) as dt_mes_analisis
      ,state
      ,last_state
      ,state != "cuenta_cerrada" as cliente
      ,state = "activo" as cliente_ready
    FROM {{source('churn','tablon_monthly_eventos_churn')}}  --`tenpo-bi.churn.tablon_monthly_eventos_churn`
), joined as (
    SELECT 
      pivote.user
      ,pivote.mes_camada
      ,dt_mes_camada
      ,churn_vectors.mes_analisis
      ,dt_mes_analisis
      ,churn_vectors.mes_analisis - pivote.mes_camada as diferencia_meses
      ,cliente
      ,cliente_ready
    FROM onboardings pivote
    LEFT JOIN churn_vectors churn_vectors ON pivote.user = churn_vectors.user AND pivote.mes_camada <= churn_vectors.mes_analisis
)
  
SELECT 
  date(dt_mes_camada) as dt_mes_camada
  ,date(dt_mes_analisis) as dt_mes_analisis
  ,diferencia_meses
  ,count(distinct user ) as suscritos
  ,count(distinct if(cliente, user, null)) as clientes
  ,count(distinct if(cliente_ready, user, null)) as clientes_ready
FROM joined
GROUP BY
   1,2,3