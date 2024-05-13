{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}

WITH 
  dimensiones as (
    SELECT DISTINCT
      tenpo_uuid id_usuario,
      tenpo_user_persona  dimension,
    FROM {{source('tablones_analisis','tablon_weekly_vectores_usuarios')}} 
    WHERE 
      Fecha_Fin_Analisis = (SELECT MAX(Fecha_Fin_Analisis) FROM {{source('tablones_analisis','tablon_weekly_vectores_usuarios')}}  )
    ),
  fechas as (
    SELECT fecha
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2020-01-01'), CURRENT_DATE(), INTERVAL 1 DAY)) as fecha
    ),
  datos_base_churn as (
    SELECT DISTINCT
      fecha,
      dimension,
      EXTRACT(YEAR from fecha) as year,
      EXTRACT(MONTH from fecha) as month,
      EXTRACT(DAY from fecha) as monthday,
      EXTRACT(ISOWEEK from fecha) as week,
      EXTRACT(DAYOFWEEK from fecha) as weekday,
      COUNT(DISTINCT IF( cuenta_cerrada is false and fecha = fecha_ob,  user , null))  registros_ok,
      COUNT(DISTINCT IF( cuenta_cerrada is true and fecha_cierre  = fecha ,  user , null))  cierres_cuenta,
      COUNT(DISTINCT IF( fecha = app_uninstall_date,  user , null))  desinstalaciones,
      COUNT(DISTINCT IF( fecha_retorno = fecha,  user , null))  retornos,
      COUNT(DISTINCT IF( churn = 1 AND fecha = fecha_churn,  user , null))  fugas,
    FROM {{ ref('churn_app') }}  --`tenpo-bi-prod.churn.churn_app` 
    JOIN fechas on 1 =1
    LEFT JOIN dimensiones on id_usuario = user
    GROUP BY 
      1,2
      ),
  running_calculations as (
    SELECT
      datos_base_churn.*,
      cierres_cuenta + desinstalaciones renuncias,
      SUM(registros_ok - cierres_cuenta - desinstalaciones + retornos) OVER (PARTITION BY dimension ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS clientes_acum,
    FROM datos_base_churn
    ),
  calculo_lags as (    
      SELECT 
        running_calculations.* EXCEPT(retornos, renuncias),
        -- CLIENTES PERIODOS ANTERIORES
        LAG(clientes_acum) OVER (PARTITION BY dimension ORDER BY fecha ASC) clientes_dia_anterior, 
        LAG(clientes_acum) OVER (PARTITION BY dimension, weekday ORDER BY fecha ASC) clientes_semana_anterior,
        IF(monthday != 31, LAG(clientes_acum) OVER (PARTITION BY dimension, monthday ORDER BY fecha ASC), null) clientes_mes_anterior,
        -- RENUNCIAS: DESINSTALACIONES + FUGA DURA
        renuncias,
        LAG(renuncias) OVER (PARTITION BY dimension ORDER BY fecha ASC) renuncias_dia_anterior, 
        IF(fecha = "2021-01-01", null, SUM( renuncias) OVER (PARTITION BY dimension,  year, week ORDER BY fecha ASC)) renuncias_semana,
        SUM(renuncias) OVER (PARTITION BY dimension, year, month ORDER BY fecha ASC) renuncias_mes,
        -- USUARIOS QUE VUELVEN A TRANSACCIONAR/INSTALAR LA APP/HACER LOGIN: Fecha de retorno es la misma que la fecha de desinstalación
        retornos,
        LAG(retornos) OVER (PARTITION BY dimension ORDER BY fecha ASC) retornos_dia_anterior, 
        IF(fecha = "2021-01-01", null, SUM(retornos) OVER (PARTITION BY dimension, year, week ORDER BY fecha ASC)) retornos_semana,
        SUM(retornos) OVER (PARTITION BY dimension, year, month ORDER BY fecha ASC) retornos_mes,
      FROM running_calculations 
     )
     
     SELECT 
      *,
      safe_divide((renuncias - retornos),clientes_dia_anterior )churn_diario,
      safe_divide((renuncias_semana - retornos_semana),clientes_semana_anterior) churn_semanal,
      safe_divide((renuncias_mes - retornos_mes),clientes_mes_anterior) churn_mensual
     FROM calculo_lags
     WHERE dimension is not null
  