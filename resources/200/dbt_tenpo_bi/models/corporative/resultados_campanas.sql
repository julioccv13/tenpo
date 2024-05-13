{{ config(materialized='table',  tags=["daily", "bi"]) }}
with 
  usuarios_tenpo as (
    SELECT distinct
      u.email as correo,
      u.id, 
      u.ob_completed_at,
      u.created_at,
      edad,
      u.state,
      d.gender,
      last_ndd_service_bm,
      sigla_region region,
      comuna,
    FROM {{ref('users_tenpo')}} u
      LEFT JOIN {{ref('demographics')}} d ON (u.id=d.id_usuario)
      LEFT JOIN {{ref("users_allservices")}} s using (id)
  ),
  economics as (
    SELECT distinct
      fecha as fecha_trx,
      e.nombre,
      monto,
      trx_id,
      linea,
      comercio,
      e.email as correo,
      edad,
      sigla_region region,
      comuna,
      last_ndd_service_bm,
    FROM {{ref("economics")}} e
      JOIN {{ref('users_tenpo')}} u ON (u.id=user)
      LEFT JOIN {{ref('demographics')}} d ON (u.id=d.id_usuario)
      LEFT JOIN {{ref("users_allservices")}} s using (id)
  ), 
  usuarios_cupones as (
    select distinct 
      r.id as id_cupon,
      IF(r.confirmed, "Quemado", "Ingresado") as estado_cupon,
      c.name as nombre_cupon,
      r.redeem_date,
      {{ hash_sensible_data('email') }} as correo
    from {{source('payment_loyalty','redeems')}} r
      JOIN {{source('payment_loyalty','coupons')}} c on (r.id_coupon=c.id)
      JOIN {{ref('users_tenpo')}} u ON (u.id=r.id_user)
      
  ),
  campanas as (
    SELECT 
      correo,
      campana as nombre_campana,
      fecha_ini  as fecha_inicio_campana,
      fecha_fin as fecha_fin_campana,
      linea as linea_campana,
    FROM {{ref('consolidado_base_campanas')}}
  )
SELECT distinct
  correo,
  nombre_campana,
  fecha_inicio_campana,
  fecha_fin_campana,
  id,
  ob_completed_at,
  created_at,
  IF (usuarios_post_campana.edad is not null, usuarios_post_campana.edad, economics_post_campana.edad) as edad,
  state,
  gender,
  IF (usuarios_post_campana.last_ndd_service_bm is not null, usuarios_post_campana.last_ndd_service_bm, economics_post_campana.last_ndd_service_bm) as last_ndd_service_bm,
  IF (usuarios_post_campana.comuna is not null, usuarios_post_campana.comuna, economics_post_campana.comuna) as comuna,
  IF (usuarios_post_campana.region is not null, usuarios_post_campana.region, economics_post_campana.region) as region,
  fecha_trx,
  nombre,
  monto,
  trx_id,
  linea,
  comercio,
  id_cupon,
  linea_campana,
  estado_cupon,
  nombre_cupon,
  redeem_date,
FROM  campanas
  LEFT JOIN (
    select 
      t1.*,
      nombre_campana,
      fecha_inicio_campana,
      fecha_fin_campana
    from usuarios_tenpo as t1 
      JOIN campanas using (correo) 
    where CAST(ob_completed_at AS DATE) between fecha_inicio_campana and fecha_fin_campana
  ) as usuarios_post_campana USING (correo,nombre_campana,fecha_inicio_campana,fecha_fin_campana)
  LEFT JOIN (
    select 
      t1.*,
      nombre_campana,
      fecha_inicio_campana,
      fecha_fin_campana
    from campanas 
      JOIN economics as t1 USING (correo)
    where CAST(fecha_trx AS DATE) between fecha_inicio_campana and fecha_fin_campana
  ) as economics_post_campana USING (correo,nombre_campana,fecha_inicio_campana,fecha_fin_campana)
  LEFT JOIN (
    select 
      t1.*,
      nombre_campana,
      fecha_inicio_campana,
      fecha_fin_campana
    from campanas
      JOIN usuarios_cupones as t1 USING (correo)
    where CAST(t1.redeem_date  AS DATE) between fecha_inicio_campana and fecha_fin_campana
  ) as cupones USING (correo,nombre_campana,fecha_inicio_campana,fecha_fin_campana)