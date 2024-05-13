  {% set partitions_between_to_replace = [
    'date_sub(current_date, interval 30 day)',
    'current_date'
] %}

{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'semana', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}

WITH 
  churn_mensual as (
    SELECT
      DATE_TRUNC(Fecha_Fin_Analisis_DT, week) semana
      ,Fecha_Fin_Analisis_DT
      ,user
      ,case when state = "activo" then 1 else 0 end cliente
      ,case when state in ("churneado", "cuenta_cerrada") and last_state in ("onboarding", "activo") then 1 else 0 end churn_periodo  
      ,case when last_state = "onboarding"  then 1 else 0 end ob_periodo 
      ,case when state = "cuenta_cerrada"  and last_state in ("onboarding", "activo","churneado") then 1 else 0 end cierres_cuenta_periodo 
      ,case when state = "activo" and last_state in ("churneado") then 1 else 0 end retorno_periodo  
    FROM {{source('churn','tablon_weekly_eventos_churn')}}  
    WHERE TRUE
       AND DATE(DATE_TRUNC(periodo_legible, week)) =  DATE_TRUNC(Fecha_Fin_Analisis_DT, week)
{% if is_incremental() %}
    and DATE(DATE_TRUNC(periodo_legible, week)) between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
--        AND  user = "92cc7595-dc0d-44ad-8481-b2c8028cbddb"
       ),
   usuarios_tarjetas as (
    SELECT DISTINCT
      DATE_TRUNC(usuarios_tarjetas.Fecha_Fin_Analisis_DT , week) semana
      ,usuarios_tarjetas.Fecha_Fin_Analisis_DT  
      ,CAST(IF(fecha_activacion <'2020-01-01', '2020-01-01', fecha_activacion) AS DATE) fecha_activacion
      ,usuarios_tarjetas.user 
      ,usuarios_tarjetas.tipo
      ,cliente
    FROM {{source('tarjeta','activacion_tarjeta_weekly')}}  usuarios_tarjetas
    LEFT JOIN churn_mensual ON (usuarios_tarjetas.user = churn_mensual.user AND DATE_TRUNC(usuarios_tarjetas.Fecha_Fin_Analisis_DT , week) = churn_mensual.semana)
    WHERE TRUE
      AND tipo is not null
{% if is_incremental() %}
    and DATE_TRUNC(usuarios_tarjetas.Fecha_Fin_Analisis_DT , week) between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
      --AND usuarios_tarjetas.user  = "3225bdb1-a27e-4971-b1a3-8e18e8852515"
    ),
  tarjetas as (
    SELECT
      usuarios_tarjetas.semana semana
      ,COUNT(DISTINCT usuarios_tarjetas.user ) tarjetas_activadas
      ,COUNT(DISTINCT case when cliente = 1 then usuarios_tarjetas.user else null end) cuenta_clientes_tarjeta
      ,COUNT(DISTINCT case when cliente = 1 and tipo = "PHYSICAL" then usuarios_tarjetas.user else null end) cuenta_physical
      ,COUNT(DISTINCT case when cliente = 1 and tipo = "VIRTUAL" then usuarios_tarjetas.user else null end) cuenta_virtual
    FROM usuarios_tarjetas
    where true
{% if is_incremental() %}
    and usuarios_tarjetas.semana between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
    GROUP BY 
      1 
      ),
  compras_exitosas as (
    SELECT
      DATE_TRUNC(fecha , week) semana
      ,COUNT(DISTINCT user ) cuenta_compras_exitosas
    FROM {{ref('economics')}} 
    WHERE 
      linea in ( 'mastercard', 'mastercard_physical')
{% if is_incremental() %}
    and DATE_TRUNC(fecha , week) between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}
    GROUP BY 
      1
      )
   
   SELECT
    *
   FROM tarjetas 
   LEFT JOIN compras_exitosas USING(semana)
