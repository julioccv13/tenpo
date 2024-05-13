

{{   config(tags=["daily", "bi"], materialized='table') }}

WITH 
  fechas as (
    SELECT 
      fecha_max,
      CAST(FORMAT_DATE('%Y-%m-01', fecha_max ) AS DATE) fecha_min,
    FROM UNNEST(
        GENERATE_DATE_ARRAY(DATE('2020-01-01'), DATE_SUB(DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY), INTERVAL 1 DAY)) as fecha_max
    ),
  active_buyers as (
    SELECT DISTINCT
      CAST(FORMAT_DATE('%Y-%m-01', Fecha_Fin_Analisis_DT ) AS DATE) fecha_min,
      Fecha_Fin_Analisis_DT as fecha_max,
      COUNT( DISTINCT user) as total_ab,
    FROM {{source('productos_tenpo','tenencia_productos_tenpo')}}
    JOIN {{ ref('economics') }}  USING(user)
    WHERE 
      fecha >= CAST(FORMAT_DATE('%Y-%m-01', Fecha_Fin_Analisis_DT) AS DATE)  
      AND fecha <= Fecha_Fin_Analisis_DT    
      AND lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
    GROUP BY 1,2
    ),
  active_buyers_acum as (
    SELECT 
      *,
      LAST_VALUE(total_ab) OVER (PARTITION BY fecha_min ORDER BY fecha_max ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) activos_max
    FROM active_buyers
    GROUP BY 
      1,2,3
    ),     
  estimacion_active_buyers as (
    SELECT
      fecha_min,
      fecha_max,
      total_ab,
      EXTRACT(DAY from fecha_max) dia_mes,
    FROM fechas 
    LEFT JOIN active_buyers_acum USING(fecha_max, fecha_min) 
    ),   
  -->>>>>>>>>>>>>>>>>ESTIMACIÓN <<<<<<<<<<<<<<<<<<
  parametros_ecuacion_mes_actual as (
    SELECT
      potencia, 
      beta
    FROM {{ source('active_buyers', 'active_buyers_equation') }}  --`tenpo-bi-prod.active_buyers.active_buyers_equation` 
    WHERE 
      Fecha_Inicio_Ejec_DT = CAST(FORMAT_DATE('%Y-%m-01', CURRENT_DATE()) AS DATE) 
    ),
  crecimiento as (
      SELECT
        *,
        POWER(dia_mes, potencia)*beta porc_crecim,
      FROM estimacion_active_buyers
      LEFT JOIN parametros_ecuacion_mes_actual on 1 = 1
    ),   
  ab_inicio_mes_actual as (    
     SELECT 
      total_ab 
     FROM estimacion_active_buyers WHERE fecha_max = fecha_min AND fecha_min = CAST(FORMAT_DATE('%Y-%m-01', CURRENT_DATE()) AS DATE) 
    ),
  ab_mas_reciente as (
     SELECT 
      MAX(total_ab) total_ab
     FROM estimacion_active_buyers 
     WHERE
      total_ab is not null
      AND fecha_min = CAST(FORMAT_DATE('%Y-%m-01', CURRENT_DATE()) AS DATE) 
  )

          
SELECT
  *,
  MAX(ab_proy)  OVER (PARTITION BY fecha_min) max_ab_proy
FROM(
  SELECT
    a.*,
    EXP(SUM(LN((1 + COALESCE(CASE WHEN a.total_ab is null THEN porc_crecim ELSE 0 END, 1))))  OVER (PARTITION BY fecha_min ORDER BY fecha_max ASC ) ) AS accumulated,
    CAST(IF(a.total_ab is null, EXP(SUM(LN((1 + COALESCE(CASE WHEN a.total_ab is null THEN porc_crecim ELSE 0 END, 1))))  OVER (PARTITION BY fecha_min ORDER BY fecha_max ASC ) ) * ab_mas_reciente.total_ab , a.total_ab ) as INT64) ab_proy,
  FROM crecimiento a
  LEFT JOIN ab_inicio_mes_actual ON 1=1
  LEFT JOIN ab_mas_reciente ON 1 = 1
)
ORDER BY 
  2 DESC