{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}
SELECT
  FORMAT_DATE('%Y-%m', mes_camada) mes_camada,
  MAX(IF( s1 = 0 , churn_semanal, null))  s0 ,
  MAX(IF( s1 = 1 , churn_semanal, null))  s1 ,
  MAX(IF( s2 = 1 , churn_semanal, null))  s2 ,
  MAX(IF( s3 = 1 , churn_semanal, null))  s3 ,
  MAX(IF( s4 = 1 , churn_semanal, null))  s4 ,
  MAX(IF( s5 = 1 , churn_semanal, null))  s5 ,
  MAX(IF( s6 = 1 , churn_semanal, null))  s6 ,
  MAX(IF( s7 = 1 , churn_semanal, null))  s7 ,
  MAX(IF( s8 = 1 , churn_semanal, null))  s8 ,
  MAX(IF( s9 = 1 , churn_semanal, null))  s9 ,
  MAX(IF( s10 = 1 , churn_semanal, null)) s10,
  MAX(IF( s11 = 1 , churn_semanal, null)) s11,
  MAX(IF( s12 = 1 , churn_semanal, null)) s12,
  MAX(IF( s13 = 1 , churn_semanal, null)) s13,
  MAX(IF( s14 = 1 , churn_semanal, null)) s14,
  MAX(IF( s15 = 1 , churn_semanal, null)) s15,
  MAX(IF( s16 = 1 , churn_semanal, null)) s16 ,
  MAX(IF( s17 = 1 , churn_semanal, null)) s17 ,
  MAX(IF( s18 = 1 , churn_semanal, null)) s18,
  MAX(IF( s19 = 1 , churn_semanal, null)) s19,
  MAX(IF( s20 = 1 , churn_semanal, null)) s20,
  MAX(IF( s21 = 1 , churn_semanal, null)) s21,
  MAX(IF( s22 = 1 , churn_semanal, null)) s22,
  MAX(IF( s23 = 1 , churn_semanal, null)) s23,
  MAX(IF( s24 = 1 , churn_semanal, null)) s24,
  MAX(IF( s25 = 1 , churn_semanal, null)) s25,
  MAX(IF( s26 = 1 , churn_semanal, null)) s26,
  MAX(IF( s27 = 1 , churn_semanal, null)) s27,
  MAX(IF( s28 = 1 , churn_semanal, null)) s28,
  MAX(IF( s29 = 1 , churn_semanal, null)) s29,
FROM
  (
  SELECT
    CAST(dimension as date) mes_camada,
    EXTRACT(ISOWEEK FROM fecha) semana,
    EXTRACT(ISOWEEK FROM CAST(dimension as date)) semana_inicio_camada,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in ( 0 ) , 1,0) s0,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-52, 1 ) , 1,0) s1,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-51, 2 ) , 1,0)  s2,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-50, 3 ) , 1,0) s3,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-49, 4 ) , 1,0)  s4,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-48, 5 ) , 1,0)  s5,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-47, 6 ) , 1,0)  s6,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-46, 7 ) , 1,0)  s7,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-45, 8 ) , 1,0)  s8,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-44, 9 ) , 1,0)  s9,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-43, 10) , 1,0) s10,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-42, 11) , 1,0) s11,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-41, 12) , 1,0) s12,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-40, 13) , 1,0) s13,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-39, 14) , 1,0) s14,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-38, 15) , 1,0) s15,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-37, 16) , 1,0)  s16,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-36, 17) , 1,0)  s17,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-35, 18) , 1,0) s18,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-34, 19) , 1,0) s19,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-33, 20) , 1,0) s20,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-32, 21) , 1,0) s21,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-31, 22) , 1,0) s22,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-30, 23) , 1,0) s23,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-29, 24) , 1,0) s24,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-28, 25) , 1,0) s25,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-27, 26) , 1,0) s26,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-26, 27) , 1,0) s27,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-25, 28) , 1,0) s28,
    IF( EXTRACT(ISOWEEK FROM fecha)-EXTRACT(ISOWEEK FROM CAST(dimension as DATE)) in (-24, 29) , 1,0) s29,
    MAX(churn_semanal) churn_semanal
  FROM {{ ref('churn_ratio_camada') }} 
  WHERE 
    weekday in (1)
    AND churn_semanal is not null
    AND dimension >= '2020-06'
  GROUP BY 
    fecha, dimension
  )
GROUP BY
  1
ORDER BY 
  1 ASC
