{{ config( 
    tags=["hourly", "bi", "sac"],
    materialized='table')
}}

WITH 
  tickets_creados as (
    SELECT 
     date(DATE_TRUNC(creacion,isoweek)) semana_creacion
     ,EXTRACT(isoweek from creacion) as semana_creacion_num
     ,count(distinct id) tickets
    FROM {{ ref('tickets_freshdesk') }} --`tenpo-bi-prod.external.tickets_freshdesk` 
    GROUP BY 1,2
    ORDER BY 1 desc
    ),
   tickets_cerrados as(
    SELECT 
     date(DATE_TRUNC(cierre,isoweek)) semana_cierre
     ,EXTRACT(isoweek from cierre) as semana_cierre_num
     ,count(distinct id) tickets
    FROM {{ ref('tickets_freshdesk') }} --`tenpo-bi-prod.external.tickets_freshdesk` 
    WHERE 
      lower(estado) = 'closed'
    GROUP BY 1,2
    ORDER BY 1 desc
    
    )
    
    SELECT
      a.semana_creacion semana
      ,a.semana_creacion_num semana_num
      ,a.tickets as tickets_creados
      ,b.tickets as tickets_cerrados
    FROM tickets_creados a
    LEFT JOIN tickets_cerrados b on semana_creacion = semana_cierre and semana_creacion_num = semana_cierre_num
    ORDER BY 1 DESC