{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

WITH
    economics_app AS (
  
        SELECT 
            IF(fecha < '2020-01-01' , '2020-01-01' , fecha) fecha,
            nombre,
            linea,
            user
        FROM  {{ ref('economics') }}
        WHERE 
            linea in ('cash_in', 'mastercard', 'cash_out', 'utility_payments', 'top_ups', 'paypal' , 'p2p', 'p2p_received')
            AND fecha <= current_date()
      ),
    
    onboardings as (
      SELECT 
        id uuid,
        IF(DATE(ob_completed_at, "America/Santiago") < '2020-01-01' , '2020-01-01' ,DATE(ob_completed_at, "America/Santiago")) fecha_ob
      FROM {{ ref('users_tenpo') }}
      WHERE 
        state in (4,7,8,21,22)
             ),
             
    log_visitas as(
      SELECT
        user,
        linea,
        DATE_DIFF( CAST(FORMAT_DATE('%Y-%m-01',fecha) AS DATE) ,  '2020-01-01' , MONTH) mes_visita,
      FROM economics_app 
      JOIN onboardings ON uuid = user
      GROUP BY 1,2,3
      ORDER BY 
        1 DESC, 2 DESC
    ),
    
    lapso_tiempo AS (
      SELECT
        user,
        mes_visita,
        LAG(mes_visita) OVER (PARTITION BY user ORDER BY user, mes_visita ASC)  lapso
      FROM log_visitas
    ),
    
   diferencia_tiempo AS (
     SELECT 
       *,
       mes_visita - lapso   as diferencia
     FROM lapso_tiempo 
    ),
    
   categorizacion_clientes AS(
      SELECT
        user,
        mes_visita,
      CASE
        WHEN diferencia = 1 THEN 'superactivo'
        WHEN diferencia = 2 THEN 'activo'
        WHEN diferencia > 2 THEN 'dormido'
        WHEN diferencia IS NULL THEN 'nuevo'
      END AS cust_type
      FROM diferencia_tiempo
    ),
    
    primera_visita AS (

      SELECT 
        user,
        min(mes_visita) AS primer_mes_visita
      FROM log_visitas
      WHERE linea in ('cash_in')
      GROUP BY 
        1 
    ),
    
    usuarios_nuevos as (
      SELECT
        primer_mes_visita,
        COUNT(DISTINCT user) AS nuevos_usuarios
      FROM primera_visita
      GROUP BY
        1
    
    )
-->>>>>>>>>>> Cálculo del % de retención por mes <<<<<<<<<<<<<<<
   
   SELECT
    first_month,
    CASE 
    WHEN first_month in (0,12,24) THEN 1
    WHEN first_month in (1,13,25) THEN 2
    WHEN first_month in (2,14,26) THEN 3
    WHEN first_month in (3,15,27) THEN 4
    WHEN first_month in (4,16,28) THEN 5
    WHEN first_month in (5,17,29) THEN 6
    WHEN first_month in (6,18,30) THEN 7
    WHEN first_month in (7,19,31) THEN 8
    WHEN first_month in (8,20,32) THEN 9
    WHEN first_month in (9,21,33) THEN 10
    WHEN first_month in (10,22,34) THEN 11
    WHEN first_month in (11,23,35) THEN 12
    END AS first_month_recod,
    (first_month + 1) first_month_recod_2,
    new_users,
    retention_month,
    retained,
    retention_percent
   FROM(   
   SELECT
      usuarios_nuevos.primer_mes_visita first_month,
      nuevos_usuarios new_users,
      visit_tracker.mes_visita- log_visitas.mes_visita retention_month,
      COUNT(DISTINCT visit_tracker.user) AS  retained,
      COUNT(DISTINCT visit_tracker.user)/nuevos_usuarios AS  retention_percent
    FROM primera_visita
    LEFT JOIN log_visitas USING(user)
    LEFT JOIN log_visitas AS visit_tracker ON log_visitas.user = visit_tracker.user AND log_visitas.mes_visita < visit_tracker.mes_visita
    LEFT JOIN usuarios_nuevos ON usuarios_nuevos.primer_mes_visita = primera_visita.primer_mes_visita
    WHERE usuarios_nuevos.primer_mes_visita + visit_tracker.mes_visita- log_visitas.mes_visita <= DATE_DIFF( CAST(FORMAT_DATE('%Y-%m-01',CURRENT_DATE()) AS DATE) ,  '2020-01-01' , MONTH)
    GROUP BY 1,2,3
)