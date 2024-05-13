{{ 
  config(
    materialized='table',
  ) 
}}

WITH tickets AS

(SELECT 
     DISTINCT 
     email, 
     --'open ticket' as reason, 
     --true as no_molestar,
     ticket_updated_at,
     ROW_NUMBER() OVER (PARTITION BY email ORDER BY ticket_updated_at DESC) row_no,
     t1.*	 
FROM {{source('freshdesk','freshdesk')}} t1
JOIN {{ source('tenpo_users', 'users') }} u on u.id = user_id   
--WHERE ticket_portal_name IN ('Tenpo','Centro de ayuda Tenpo')
--AND ticket_ticket_type NOT IN ('Consulta','Consultas Generales','Otros','Sugerencias','Sugerencias Recargas','Robo o pérdida celular','Ticket Prueba')
--AND ticket_status NOT IN ('Closed','Pendiente respuesta cliente','Resolved')
WHERE ticket_portal_name IN ('Tenpo','Centro de ayuda Tenpo')
AND LENGTH(user_id) > 1)

SELECT 
      DISTINCT
      email,
     'open ticket' as reason, 
      true as no_molestar, 
FROM tickets
WHERE row_no = 1
AND ticket_status NOT IN ('Closed','Pendiente respuesta cliente','Resolved')
AND ticket_ticket_type NOT IN ('Consulta','Consultas Generales','Otros','Sugerencias','Sugerencias Recargas','Ticket Prueba')


/* EXCLUIR A LOS QUE SE INTERSECTAN (TIENE TICKET ABIERTO Y SON CHURN) 
AND email NOT IN (SELECT 
                        DISTINCT 
                        email 
                  FROM {{ ref('today_open_tickets_churn') }}) */