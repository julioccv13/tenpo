{{ config(materialized='ephemeral') }}

SELECT DISTINCT
  MD5(IF(trx.id_usuario  > 0, CAST (trx.id_usuario AS STRING), (IF(trx.id_usuario < 0 AND trx.correo_usuario <> "",trx.correo_usuario,rec.suscriptor)))) as id_usr,
  MD5(rec.suscriptor) as id_sus,
  {{ hash_sensible_data('trx.correo_usuario') }} as correo_usuario, 
  CASE 
    WHEN trx.id_usuario  > 0 THEN 'Registrado'
    WHEN trx.id_usuario < 0 AND trx.correo_usuario <> "" THEN 'Semiregistrado'
    ELSE 'Anónimo' 
    END
    AS tipo_usuario,
  trx.fecha_creacion as fecha,
  cast(trx.id as string) as id_trx,
  trx.monto as monto,
  ori.nombre as origen,
  trx.plataforma,
  trx.id_medio_pago,
  trx.id_estado as id_est_trx,
  CASE 
    WHEN est_t.glosa = "FINALIZADO" THEN "5. Finalizado"
    WHEN est_t.glosa = "CREADO" THEN "1. Creado"
    WHEN est_t.glosa = "MEDIOSELECCIONADO" THEN "2. Medio de pago seleccionado"
    WHEN est_t.glosa = "PAGOAPROBADO" THEN "3. Pago aprobado"
    WHEN est_t.glosa = "PAGORECHAZADO" THEN "4. Pago rechazado"
    WHEN est_t.glosa = "REVERSADO" THEN "6. Reversado"
    END as trx_estado,
  rec.id_estado as id_est_rec,
  est_r.glosa as rec_estado,
  mpa.nombre as medio_pago,
  concat(est_t.glosa," - ",est_r.glosa) as status,
  tprod.nombre as categoria,
  ope.nombre as operador,		
  IF(trx.id_estado >= 16, 1,0) AS creado,
  IF(trx.id_estado >= 17, 1,0) AS medio_seleccionado,
  IF(trx.id_estado = 18 OR trx.id_estado = 20, 1,0) AS pago_aprobado,
  IF(trx.id_estado = 20, 1,0) AS finalizado,
  IF(trx.id_estado = 19 OR trx.id_estado = 21 OR trx.id_estado = 22, 1,0) AS otros,
FROM
  {{source("topups_web","ref_transaccion")}} trx
JOIN
  {{source("topups_web","ref_recarga")}} rec ON trx.id=rec.id_transaccion
JOIN
  {{source('topups_web','ref_estado')}} est_t ON trx.id_estado=est_t.id
JOIN
  {{source('topups_web','ref_medio_pago')}} mpa ON trx.id_medio_pago=mpa.id
JOIN
  {{source('topups_web','ref_estado')}} est_r ON rec.id_estado=est_r.id
JOIN
  {{source('topups_web','ref_origen')}} ori ON ori.id=trx.id_origen 
JOIN
  {{source('topups_web','ref_producto')}} prod ON prod.id = rec.id_producto 
JOIN
  {{source('topups_web','ref_operador')}} ope ON ope.id = prod.id_operador
JOIN
  {{source('topups_web','ref_comisiones')}} com ON com.id_producto = prod.id 
JOIN
  {{source('topups_web','ref_tipo_producto')}} tprod ON tprod.id = prod.id_tipo_producto 
WHERE trx.id_origen IN (1,2,5) -- t.id_estado = 20 AND r.id_estado = 27