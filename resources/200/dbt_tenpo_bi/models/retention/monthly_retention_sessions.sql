{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
  ) 
}}

WITH 
  visitas_clevertap as (
    SELECT DISTINCT 
      PARSE_DATE("%Y%m%d", CAST(date as STRING)) fecha,
      id user,
      session_id,
      session_lenght,
      ROW_NUMBER() OVER (PARTITION BY  email, session_id , date ORDER BY fecha_hora ) ro_n
    FROM {{source('clevertap','events')}}
    JOIN {{ ref('users_tenpo') }} USING(email) --{{ref('users_tenpo')}}
    WHERE
      event = 'Session Concluded'
      AND state in (4,7,8,21,22)
      AND ob_completed_at is not null
      AND session_lenght > 0
      AND session_lenght <= 1200
         ),
  visitas_unicas as (
    SELECT 
       * EXCEPT(ro_n)
    FROM visitas_clevertap
    WHERE 
      ro_n = 1
  ),
  log_visitas as(
    SELECT
      user,
      DATE_DIFF( CAST(FORMAT_DATE('%Y-%m-01',fecha) AS DATE) ,  '2020-01-01' , MONTH) mes_visita,
    FROM visitas_unicas 
    GROUP BY 
      1,2
      ),
  
  lapso_tiempo AS (
    SELECT
      user,
      mes_visita,
      LEAD(mes_visita) OVER (PARTITION BY user ORDER BY user, mes_visita ASC)  lapso
    FROM log_visitas
  ),
  diferencia_tiempo AS (
    SELECT 
      *,
      lapso - mes_visita   as diferencia
    FROM lapso_tiempo 
  ),
  categorizacion_clientes AS(
    SELECT
      user,
      mes_visita,
      CASE
      WHEN diferencia = 1 THEN 'retenido'
      WHEN diferencia > 1 THEN 'dormido'
      WHEN diferencia IS NULL THEN 'perdido'
    END AS cust_type
    FROM diferencia_tiempo
  ),  
  primera_visita AS (  
    SELECT 
      user,
      MIN(mes_visita) AS primer_mes_visita
    FROM log_visitas
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

-->>>>>>>>>>> Cálculo del % de usuarios que retornan al siguiente mes <<<<<<<<<<<<<<<
SELECT 
  mes_visita,
  CASE 
   WHEN mes_visita in (0,12,24) THEN 1
   WHEN mes_visita in (1,13,25) THEN 2
   WHEN mes_visita in (2,14,26) THEN 3
   WHEN mes_visita in (3,15,27) THEN 4
   WHEN mes_visita in (4,16,28) THEN 5
   WHEN mes_visita in (5,17,29) THEN 6
   WHEN mes_visita in (6,18,30) THEN 7
   WHEN mes_visita in (7,19,31) THEN 8
   WHEN mes_visita in (8,20,32) THEN 9
   WHEN mes_visita in (9,21,33) THEN 10
   WHEN mes_visita in (10,22,34) THEN 11
   WHEN mes_visita in (11,23,35) THEN 12
   END AS mes_visita_recod,
  (mes_visita + 1) mes_visita_recod_2,
  COUNT(DISTINCT user) total_usuarios,
  COUNT(DISTINCT IF( cust_type = 'retenido', user, null)) retenido,
  COUNT(DISTINCT IF( cust_type = 'dormido', user, null)) dormido,
  COUNT(DISTINCT IF( cust_type = 'perdido', user, null)) perdido,
  COUNT(DISTINCT IF( cust_type = 'retenido', user, null))/COUNT(DISTINCT user) as retencion,
  COUNT(DISTINCT IF( cust_type = 'perdido', user, null))/COUNT(DISTINCT user) as churn
FROM categorizacion_clientes
GROUP BY 
  1,2,3