{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
  ) 
}}
WITH 
  target as (
    SELECT 
      nh.id
      ,body.body.resolucion_tx 
      ,descripcion_sia
      ,false f_aprobada
      ,timestamp_seconds(CAST(nh.created_at as INT64) ) as trx_timestamp
      ,nh.body.header.cuenta id_cuenta
      , CASE 
         WHEN body.body.tipo_tx in (1,11) then 'compra'
         WHEN body.body.tipo_tx = 55 THEN 'suscripción'
         WHEN body.body.tipo_tx = 2 THEN 'devolución'
         END AS tipo_tx
      ,CASE
        WHEN nh.body.body.country_iso_3266_code = 152 THEN 'nacional'
        ELSE 'internacional'
        END AS origen_tx
      ,u.uuid user
      ,cast(body.body.sd_value as float64) monto
      ,body.body.merchant_name
      ,body.body.merchant_code
    FROM {{ source('notifications','notifications_history_v2') }}  nh
    JOIN {{ source('aux','resoluciones_sia') }}  ON nh.body.body.resolucion_tx = resolucion_sia
    JOIN {{ source('prepago','prp_cuenta') }}   c ON CAST(nh.body.header.cuenta as INT64) = CAST(RIGHT(CAST(c.cuenta as STRING), 7) as INT64)
    JOIN {{ source('prepago','prp_usuario') }}  u ON c.id_usuario = u.id
    WHERE
      body.body.resolucion_tx not in ( 1,400)

    UNION ALL

    SELECT
      m.id 
      ,1 as resolucion_tx
      ,"APROBADA" descripcion_sia
      ,true as f_aprobada
      ,m.fecha_creacion trx_timestamp 
      ,CAST(RIGHT(CAST(c.cuenta as STRING), 7) as INT64) id_cuenta
      ,case when tipofac in  (3010,3011,3012,3030) then 'devolución' 
       when tipofac in (3007, 3028  ) then 'compra'
       when tipofac in (3006,3029) then 'suscripción' 
       when tipofac in (3009, 3031, 5) then 'nacional'
       end as tipo_tx 
      ,case when tipofac in (3009, 3031, 5 ) then 'nacional' 
       else 'internacional' 
       end as  origen_tx
      ,u.uuid user
      ,cast(monto as float64) monto
      ,nomcomred merchant_name
      ,codcom merchant_code
    FROM {{source('prepago','prp_cuenta')}}  c
      JOIN {{source('prepago','prp_usuario')}}  u ON c.id_usuario = u.id
      JOIN {{source('prepago','prp_tarjeta')}}  t ON c.id = t.id_cuenta
      JOIN {{source('prepago','prp_movimiento')}}  m ON m.id_tarjeta = t.id
    WHERE 
      tipofac in (3006,3007,3028,3029,3009, 3031,5,3010,3011,3012,3030) 
      AND (m.estado  in ('PROCESS_OK','AUTHORIZED') 
       OR (m.estado = 'NOTIFIED' AND DATE(m.fecha_creacion  , "America/Santiago") BETWEEN DATE_SUB(CURRENT_DATE("America/Santiago"), INTERVAL 3 DAY) AND CURRENT_DATE("America/Santiago")))
      AND indnorcor  = 0
      ),
  dedup as (
    SELECT
      *
      ,ROW_NUMBER() OVER (PARTITION BY id || user || monto || merchant_name ORDER BY trx_timestamp desc) row_no_actualizacion
    FROM target
    )
  

  SELECT DISTINCT
    * EXCEPT(row_no_actualizacion)
  FROM dedup
  WHERE 
    row_no_actualizacion = 1
