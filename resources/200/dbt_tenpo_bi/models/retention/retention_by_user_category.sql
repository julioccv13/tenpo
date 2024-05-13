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
        monto, 
        trx_id, 
        user, 
        IF(linea LIKE '%p2p%', 'p2p', linea) linea,
        canal, 
        comercio
      FROM {{ ref('economics') }}  
      WHERE 
        linea not in ('reward', 'aum_savings')
        and nombre not like '%Devolución%'
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
      DATE_DIFF( CAST(FORMAT_DATE('%Y-%m-01',fecha) AS DATE) ,  '2020-01-01' , MONTH) mes_visita,
    FROM economics_app 
    JOIN onboardings ON uuid = user
    GROUP BY 
        1,2
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
       mes_visita - lapso    as diferencia
     FROM lapso_tiempo 
     ),   
  categorizacion_clientes AS(
     SELECT
       user,
       mes_visita,
     CASE
       WHEN diferencia <= 1 THEN 'activo'
       WHEN diferencia > 1 THEN 'retorna'
       WHEN diferencia IS NULL THEN 'nuevo'
     END AS cust_type
     FROM diferencia_tiempo
     ),   
  primera_visita AS (
     SELECT 
       user,
       min(mes_visita) AS primer_mes_visita
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

-->>>>>>>>>>> N clientes por tipo por mes <<<<<<<<<<<<<<<
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
  mes_visita + 1 mes_visita_recod_2,
  cust_type,
  COUNT( DISTINCT user) cuenta
FROM categorizacion_clientes
GROUP BY 
  1, 2, 3, 4