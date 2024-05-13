{{ 
  config(
    materialized='table', 
    enabled=True,
    tags=["hourly", "bi"],
    cluster_by = ["id_user", "nombre_camp"],
    partition_by = {'field': 'fecha', 'data_type': 'timestamp'},
  ) 
}}
WITH 
    primeras_trx as (
        -- seleccionamos las 5 primeras transacciones del usuario    
        SELECT DISTINCT 
          user id_user,
          trx_timestamp fecha,
          comercio,
        FROM {{ ref('economics') }}  
        ORDER BY
          trx_timestamp DESC
        LIMIT 5
   ),
    datos AS (
        SELECT DISTINCT 
          c.*,
          fecha, 
          comercio, 
        FROM {{ ref('coupons') }}  c
        LEFT JOIN primeras_trx mov_usr ON (mov_usr.id_user=c.id_user )
    ),
    datos_economics as (
        SELECT DISTINCT 
          user id_user,
          trx_timestamp fecha,
          comercio_recod com_recod,
          linea,
          monto,
          trx_id
        FROM {{ ref('economics') }}  
        ),
    compras_previas as (
        SELECT DISTINCT  
          datos.coupon nombre_cupon,
          datos.campana nombre_camp,
          datos.created_at re_created,
          datos.mo_fecha_redeem,
          datos.coupon_type tipo_cupon,
          datos.objective objetivo,
          datos.mov_trx_to_redeem,  
          mov_usr.*, 
          datos.mo_fecha_to_redeem, 
          count(1) OVER (PARTITION BY mov_usr.id_user,datos.coupon ORDER BY mov_usr.fecha DESC) as num_compra 
        FROM datos_economics mov_usr
        JOIN datos ON (datos.id_user=mov_usr.id_user AND datos.mo_fecha_redeem>mov_usr.fecha)
        )

SELECT
 *,
FROM compras_previas 
WHERE 
  num_compra<=5

