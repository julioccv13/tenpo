{{ 
  config(
    materialized='table', 
    cluster_by= ['user','paso'],
    partition_by = {'field': 'fecha_funnel', 'data_type': 'date'},
    tags=["daily", "bi"],
  ) 
}}

WITH
  usuarios_ob_exitoso as (
    SELECT
      id user,
      DATE(ob_completed_at, "America/Santiago") as fecha_funnel,
      DATE(NULL) as fecha_delivery,
      DATE(NULL) as fecha_activacion,
      'OB exitoso' paso,
      0 num, 
      CAST(null as STRING) region,
      CAST(null as STRING) comuna,
    FROM {{ ref('users_tenpo') }}  u
    WHERE status_onboarding = 'completo'
  ), 

  tarjeta_virtual_creada as (

    SELECT
        user,
        DATE(fecha_creacion_tarjeta, "America/Santiago") as fecha_funnel,
        DATE(NULL) as fecha_delivery,
        DATE(NULL) as fecha_activacion,
        'Tarjeta virtual creada' paso,
        1 num, 
        CAST(null as STRING) region,
        CAST(null as STRING) comuna,
    FROM {{ ref('tarjetas_virtuales_mastercard') }}
  ),

  solicitud_generada as (
    SELECT distinct
      user_id user,
      DATE(dh.movement_date, "America/Santiago") as fecha_funnel,
      DATE(NULL) as fecha_delivery,
      DATE(NULL) as fecha_activacion,
      'Solicitud generada' paso,
      2 num, 
      c.region,
      a.commune_name comuna,
    FROM {{source('tenpo_physical_card','delivery_history')}} dh
    JOIN {{source('tenpo_physical_card','card_order')}} c ON dh.card_order_id = c.id
    LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
    WHERE dh.delivery_status_id = 1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dh.movement_date ASC) = 1
  ),
  tarjeta_normalizada as (
    SELECT distinct
      user_id user,
      DATE(dh.movement_date, "America/Santiago") as fecha_funnel,
      DATE(NULL) as fecha_delivery,
      DATE(NULL) as fecha_activacion,
      'Tarjeta Normalizada' paso,
      3 num, 
      c.region,
       a.commune_name comuna,
      FROM  {{source('tenpo_physical_card','delivery_history')}} dh
    JOIN  {{source('tenpo_physical_card','card_order')}} c ON dh.card_order_id = c.id
    LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
    WHERE dh.delivery_status_id = 2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dh.movement_date ASC) = 1

  ),     
  tarjeta_fabricandose as (
    SELECT distinct
      user_id user,
      DATE(dh.movement_date, "America/Santiago") as fecha_funnel,
      DATE(NULL) as fecha_delivery,
      DATE(NULL) as fecha_activacion,
      'Tarjeta en proceso de fabricación' paso,
      4 num, 
      c.region,
       a.commune_name comuna,
      FROM  {{source('tenpo_physical_card','delivery_history')}} dh
    JOIN  {{source('tenpo_physical_card','card_order')}} c ON dh.card_order_id = c.id
    LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
    WHERE dh.delivery_status_id = 4
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dh.movement_date ASC) = 1

    ), 
  tarjeta_en_delivery as (
   SELECT
      distinct
      user_id user,
      DATE(dh.movement_date,"America/Santiago") as fecha_funnel,
      DATE(dh.movement_date,"America/Santiago") as fecha_delivery,
      DATE(NULL) as fecha_activacion,
      'Tarjeta en delivery' paso,
      5 num,
      c.region,
      a.commune_name comuna,
      FROM  {{source('tenpo_physical_card','delivery_history')}} dh
    JOIN  {{source('tenpo_physical_card','card_order')}} c ON dh.card_order_id = c.id
    LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
    WHERE dh.delivery_status_id = 5
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dh.movement_date ASC) = 1

  ), 
  tarjeta_entregada as (
    SELECT
      distinct
      user_id user,
      DATE(dh.movement_date,"America/Santiago") as fecha_funnel,
      td.fecha_delivery fecha_delivery,
      DATE(NULL) as fecha_activacion,
       'Tarjeta entregada' paso,
      6 num, 
      c.region,
      a.commune_name comuna,
      FROM  {{source('tenpo_physical_card','delivery_history')}} dh
    JOIN  {{source('tenpo_physical_card','card_order')}} c ON dh.card_order_id = c.id
    LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
    JOIN tarjeta_en_delivery td ON c.user_id = td.user
    WHERE dh.delivery_status_id = 8
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dh.movement_date ASC) = 1  
    ),
   tarjeta_activada as(    
     SELECT
      distinct
      user_id user,
      DATE(card_activation_date,"America/Santiago") as fecha_funnel,
      td.fecha_delivery fecha_delivery,
      DATE(card_activation_date,"America/Santiago") as fecha_activacion,
      'Tarjeta activada' paso,
      7 num, 
      c.region,
      a.commune_name comuna,
      FROM  {{source('tenpo_physical_card','delivery_history')}} dh
    JOIN  {{source('tenpo_physical_card','card_order')}} c ON dh.card_order_id = c.id
    LEFT JOIN {{source('tenpo_physical_card','address_region_commune')}} a ON a.id = CAST(c.commune  as INT64)
    JOIN tarjeta_en_delivery td ON c.user_id = td.user
    WHERE dh.delivery_status_id = 8
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dh.movement_date ASC) = 1  
    ),
  tarjeta_con_primera_compra as (    
    SELECT DISTINCT
      e.user,
      FIRST_VALUE(fecha) OVER (PARTITION BY e.user ORDER BY trx_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
      td.fecha_delivery fecha_delivery,
      ta.fecha_activacion,
      'Compra física exitosa' paso,
      8 num, 
      td.region,
      td.comuna,
    FROM {{ ref('economics') }} e
    JOIN tarjeta_en_delivery td ON e.user = td.user
    JOIN tarjeta_activada ta ON e.user = ta.user
    WHERE linea = "mastercard_physical"
    and lower(nombre) not like "%devolución%"
    ORDER BY 1 DESC
    )

SELECT * FROM usuarios_ob_exitoso UNION ALL
SELECT * FROM tarjeta_virtual_creada UNION ALL
SELECT * FROM solicitud_generada UNION ALL
SELECT * FROM tarjeta_normalizada UNION ALL
SELECT * FROM tarjeta_fabricandose UNION ALL
SELECT * FROM tarjeta_en_delivery UNION ALL
SELECT * FROM tarjeta_entregada UNION ALL
SELECT * FROM tarjeta_activada UNION ALL
SELECT * FROM tarjeta_con_primera_compra