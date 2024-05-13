{{ config( 
    tags=["daily", "bi"],
    materialized='table')
}}




with tickets_resueltos as (

  SELECT id, user, asunto, estado, agente
   grupo, creacion, actualizacion, resolucion,
   timestamp_mttr, mttr, 'reclamos out' as categoria,
   DATE(resolucion) as dia_ticket
  FROM {{ ref('tickets_freshdesk') }} 
  where estado in ('Resolved', 'Closed')
),
tickets_abiertos as (

  SELECT id, user, asunto, estado, agente,
  grupo, creacion, actualizacion, resolucion,
  timestamp_mttr, mttr, 'reclamos in' as categoria,
  DATE(creacion) as dia_ticket
   FROM {{ ref('tickets_freshdesk') }} 
  

),

tickets_resueltos_count as (
  SELECT dia_ticket, categoria, count(*) as cantidad_tickets, avg(mttr) as avg_mttr
  from tickets_resueltos
  group by 1, 2
  order by 1 desc
),

tickets_abiertos_count as (
  SELECT dia_ticket, categoria, count(*) as cantidad_tickets, avg(mttr) as avg_mttr
  from tickets_abiertos
  group by 1, 2
  order by 1 desc
)

-- para el grafico solo consideraar el mtt de los tickets resueltos
select * from tickets_resueltos_count
UNION ALL
select * from tickets_abiertos_count
ORDER BY 1 DESC
