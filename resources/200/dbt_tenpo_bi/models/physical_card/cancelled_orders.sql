{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  solicitudes_canceladas as(--267
     SELECT distinct 
       user
       ,card_order_id
       ,fecha_creacion  
       ,c.status_name 
       ,card_order_status
    FROM {{ ref('orders') }} c --{{ ref('orders') }} c  
     WHERE TRUE
       AND delivery_status_id in (3,5,6,7)
       AND card_order_status = "CANCELLED"
      ),
   
   solicitudes_reprocesadas_exitosas as (
    SELECT distinct 
        c.user
        ,s.fecha_creacion creacion_solicitud_cancelada
        ,s.card_order_id card_order_id_solicitud_cancelada
        ,c.card_order_id  card_order_id_solicitud_reprocesada
        ,c.fecha_creacion creacion_solicitud_reprocesada
        ,c.status_name ds_status_reprocesada
    FROM {{ ref('orders') }} c --{{ ref('orders') }} c  
    JOIN solicitudes_canceladas s on c.user = s.user and c.fecha_creacion > s.fecha_creacion  
    WHERE TRUE
        AND c.delivery_status_id  in (8)
    ),
    
   solicitudes_reprocesadas_canceladas as (
    SELECT distinct 
         c.user
        ,s.fecha_creacion creacion_solicitud_cancelada
        ,s.card_order_id card_order_id_solicitud_cancelada
        ,c.card_order_id  card_order_id_solicitud_reprocesada
        ,c.fecha_creacion creacion_solicitud_reprocesada
        ,c.status_name ds_status_reprocesada
    FROM {{ ref('orders') }} c --{{ ref('orders') }} c  
    JOIN solicitudes_canceladas s on c.user = s.user and c.fecha_creacion  > s.fecha_creacion  
    WHERE TRUE
       AND c.delivery_status_id  in (3,5,6,7)
       AND c.card_order_status = "CANCELLED"
       ),
  reprocesadas as (
    SELECT 
     *
     ,'Reprocesadas exitosas' as caso
    FROM solicitudes_reprocesadas_exitosas
    WHERE 
     TRUE
    QUALIFY row_number() OVER (PARTITION BY user order by creacion_solicitud_cancelada desc) = 1 
    
    union all
    
    SELECT 
     *
     ,'Reprocesadas canceladas' as caso
    FROM solicitudes_reprocesadas_canceladas
    WHERE 
     TRUE
    QUALIFY row_number() OVER (PARTITION BY user order by creacion_solicitud_cancelada desc) = 1 
   )
   
   SELECT distinct 
    a.user
    ,a.card_order_id card_order_id_solicitud_cancelada
    ,a.fecha_creacion creacion_solicitud_cancelada
    ,a.status_name ds_status_cancelada
    ,b.* EXCEPT(creacion_solicitud_cancelada, card_order_id_solicitud_cancelada, user, caso)
    ,COALESCE(caso, 'Sin reprocesar') caso
   FROM solicitudes_canceladas a
   LEFT JOIN reprocesadas b on card_order_id_solicitud_cancelada = card_order_id and a.user = b.user
   WHERE TRUE
   QUALIFY row_number() OVER (PARTITION BY user,a.card_order_id  order by fecha_creacion desc) = 1 
   --AND a.user = "3d3b9141-6d9f-46b0-a44d-1c74ffb56a3e"
   
   