{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH
  dau as (
    SELECT DISTINCT
     fecha, 
     COUNT(DISTINCT user) daily_active_users
    FROM {{ ref('economics') }} 
    WHERE 
      lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
    GROUP BY 1
      ),
  mau as (
    SELECT DISTINCT
      Fecha_Fin_Analisis_DT fecha,
      COUNT( DISTINCT user) as monthly_active_users,
    FROM {{source('productos_tenpo','tenencia_productos_tenpo')}}
    JOIN  {{ ref('economics') }}   USING(user)
    WHERE 
      fecha >=  date_sub(Fecha_Fin_Analisis_DT,interval 29 day)  
      AND fecha <= Fecha_Fin_Analisis_DT
      AND lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
    GROUP BY 1
  ),
  clientes as (
    SELECT DISTINCT
     DATE(Fecha_Fin_Analisis_DT) fecha,
     COUNT(DISTINCT user) clientes_acum
    FROM  {{ ref('daily_churn') }} 
    WHERE 
      churn is false
    GROUP BY 1
    )
      
  SELECT 
    dau.fecha,
    dau.daily_active_users,
    mau.monthly_active_users,
    clientes_acum,
    SAFE_DIVIDE(monthly_active_users,clientes_acum) ratio_mau_clientes,
    SAFE_DIVIDE(daily_active_users,clientes_acum) ratio_dau_clientes
  FROM dau 
  LEFT JOIN mau USING(fecha)
  LEFT JOIN clientes USING(fecha)
  WHERE 
    fecha < CURRENT_DATE()
  ORDER BY
    fecha DESC
