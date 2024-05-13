{{ 
  config(
    materialized='table', 
    cluster_by= 'user',
    tags=["hourly", "bi"],
  ) 
}}

SELECT 
  user_id user
  ,c.id card_order_id
  ,c.status card_order_status
  ,DATE(c.created , "America/Santiago") fecha_creacion
  ,DATE(c.updated  , "America/Santiago") fecha_actualizacion
  ,DATE(c.card_activation_date , "America/Santiago") fecha_activacion
  ,DATE(dh.created_date, "America/Santiago" ) fecha_delivery
  ,DATE_DIFF(dh.movement_date,c.created , DAY) dias_al_delivery
  ,ds.id delivery_status_id
  ,ds.status_name 
  ,c.region
  ,a.commune_name
  ,a.id as commune_id
  ,c.cancel_reason
  ,c.order_attempts
FROM {{source('tenpo_physical_card','card_order')}} c --`tenpo-airflow-prod.tenpo_physical_card.card_order` c
JOIN {{source('tenpo_physical_card','delivery_status')}} ds ON c.delivery_status_id = ds.id --`tenpo-airflow-prod.tenpo_physical_card.delivery_status` ds ON c.delivery_status_id = ds.id
LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
LEFT JOIN {{source('tenpo_physical_card','delivery_history')}} dh ON card_order_id = c.id and dh.delivery_status_id = ds.id
WHERE TRUE
QUALIFY row_number() over (partition by c.id order by c.updated desc) = 1