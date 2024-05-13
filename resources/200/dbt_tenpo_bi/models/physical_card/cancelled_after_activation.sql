{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

WITH 
  usuarios_tarjeta_activa as (
      SELECT distinct 
       user
       ,card_order_id
       , fecha_creacion 
       , fecha_activacion
       , card_order_status
       , fecha_actualizacion
       , delivery_status_id
       , status_name
      FROM {{ ref('orders') }} c 
      WHERE TRUE
      AND fecha_actualizacion is not null
      AND card_order_status = "FINISHED"
      AND delivery_status_id = 8
      QUALIFY row_number() over (partition by  user, card_order_id order by fecha_actualizacion DESC) = 1
      ), 
  solicitudes_canceladas as(
     SELECT distinct 
       user
       ,card_order_id
       , fecha_creacion 
       , fecha_activacion
       , fecha_actualizacion
       , delivery_status_id
       , card_order_status
       , status_name
    FROM {{ ref('orders') }}  c 
    WHERE TRUE
       AND card_order_status = "CANCELLED"
       )

 
    SELECT distinct 
         a.user
        ,a.card_order_id card_order_id_activada
        ,a.fecha_activacion fecha_activacion
        ,a.card_order_status 
        ,a.status_name
        ,b.card_order_id card_order_id_cancelada
        ,b.fecha_activacion fecha_activacion_cancelada
        ,b.status_name status_cancelada
        ,b.card_order_status card_order_status_cancelada
    FROM usuarios_tarjeta_activa a
    JOIN solicitudes_canceladas b on a.user = b.user and a.fecha_creacion < b.fecha_creacion
    WHERE TRUE