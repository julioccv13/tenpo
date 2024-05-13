{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}

SELECT
  mes_camada,
  MAX(IF(m_1 = 1 , churn_mensual, null)) mes_1,
  MAX(IF(m_2 = 1 , churn_mensual, null)) mes_2,
  MAX(IF(m_3 = 1 , churn_mensual, null)) mes_3,
  MAX(IF(m_4 = 1 , churn_mensual, null)) mes_4,
  MAX(IF(m_5 = 1 , churn_mensual, null)) mes_5,
  MAX(IF(m_6 = 1 , churn_mensual, null)) mes_6,
  MAX(IF(m_7 = 1 , churn_mensual, null)) mes_7,
  MAX(IF(m_8 = 1 , churn_mensual, null)) mes_8
FROM
  (SELECT
    FORMAT_DATE('%Y-%m', CAST(dimension AS DATE)) mes_camada,
    FORMAT_DATE('%Y-%m', fecha) mes,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 1, 1,0) m_1,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 2, 1,0) m_2,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 3, 1,0) m_3,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 4, 1,0) m_4,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 5, 1,0) m_5,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 6, 1,0) m_6,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 7, 1,0) m_7,
    IF(DATE_DIFF( fecha , CAST(dimension AS DATE) , MONTH) = 8, 1,0) m_8,
    MAX(churn_mensual) churn_mensual
  FROM {{ ref('churn_ratio_camada') }} 
  WHERE 
    monthday in (28,30,31)
    AND churn_mensual is not null
  GROUP BY 
    fecha, dimension
  )
GROUP BY 
    1
ORDER BY 
    1 ASC