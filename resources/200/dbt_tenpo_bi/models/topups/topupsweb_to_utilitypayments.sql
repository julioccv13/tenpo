{{ config(materialized='table',  tags=["daily", "bi"]) }}

WITH tenpo_users AS (
    SELECT 
      DISTINCT identifier as suscriptor,
      CASE 
        WHEN utility_id IN (36,37,35,32,59,61,51) THEN 'Celular'
        WHEN utility_id IN (39,47,48,49) THEN 'TV'
        END
      AS type,
      DATE(created) AS created_utility,
    FROM (
      SELECT identifier, utility_id, created  FROM {{source('tenpo_utility_payment','bills')}}
      UNION ALL 
      SELECT identifier, utility_id, created FROM {{source('tenpo_utility_payment','suggestions')}})
    ),
  topups_users as (
    SELECT 
    DISTINCT suscriptor,
    CASE p.id_tipo_producto 
        WHEN 1 THEN 'Celular'
        WHEN 2 THEN 'TV'
        WHEN 2 THEN 'Bam'
        WHEN 2 THEN 'Fija'
        END
      AS type,
      p.producto_recarga as company,
    MAX (DATE(r.fecha_recarga)) AS last_topup
    FROM {{source("topups_web","ref_transaccion")}}  t
      INNER JOIN {{source("topups_web","ref_recarga")}} r ON t.id = r.id_transaccion
      INNER JOIN {{source('topups_web','ref_producto')}} p ON p.id = r.id_producto 
      INNER JOIN {{source('topups_web','ref_operador')}} o ON o.id = p.id_operador
      INNER JOIN {{source('topups_web','ref_comisiones')}} c ON c.id_producto = p.id 
      INNER JOIN {{source('topups_web','ref_tipo_producto')}} tp ON tp.id = p.id_tipo_producto 
      WHERE t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5)
      GROUP BY suscriptor, type, company
      )
SELECT 
MD5(suscriptor) as identifier, type, company, last_topup, created_utility, IF(last_topup < created_utility, 'moved to plan', 'no credit') as estimated_reason 
from topups_users as t1 inner join tenpo_users as t2 
USING (suscriptor,type)