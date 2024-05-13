{{ config(materialized='ephemeral') }}

SELECT DISTINCT
  MD5(user_id) as id_usr,
  MD5(identifier) as id_sus,
  {{ hash_sensible_data('email') }} correo_usuario, 
  'Nuevo Recarga Facil' AS tipo_usuario,
  trx.created_at as fecha,
  trx.id as id_trx,
  safe_cast(rec.amount as numeric) as monto,
  'Nueva Página RF' as origen,
  'ND' as plataforma,
  safe_cast(pay.id as numeric) id_medio_pago,
  0 as id_est_trx,
  CASE 
    WHEN trx.status = "SUCCEEDED" THEN "3. Finalizado"
    WHEN trx.status = "CREATED" THEN "1. Creado"
    WHEN trx.status = "PROCESSING" THEN "2. Procesando"
    WHEN trx.status = "EXPIRED" THEN "4. Expirado"
    WHEN trx.status = "FAILED" THEN "5. Fallido"
    END as trx_estado,
  0 as id_est_rec,
  rec.status as rec_estado,
  mpa.name as medio_pago,
  concat(trx.status," - ",rec.status) as status,
  prod.name as categoria,
  ope.name as operador,		
  IF(trx.status = "CREATED", 1,0) AS creado,
  IF(trx.status = "PROCESSING", 1,0) AS medio_seleccionado,
  IF(trx.status = "SUCCEEDED", 1,0) AS pago_aprobado,
  IF(trx.status = "SUCCEEDED", 1,0) AS finalizado,
  IF(trx.status in ("EXPIRED","FAILED"), 1,0) AS otros,
FROM
  {{source('tenpo_recarga_facil','transaction')}} trx
JOIN
  {{source('tenpo_recarga_facil','topup')}} rec ON trx.id=rec.transaction_id
JOIN
  {{source('tenpo_recarga_facil','payment')}} pay ON pay.topup_id=rec.id
JOIN
  {{source('tenpo_recarga_facil','payment_method')}} mpa ON cast(pay.payment_method_id as string)=mpa.id
JOIN
  {{source('tenpo_recarga_facil','product_operator')}} pope ON pope.id = cast(rec.product_operator_id as string)
JOIN
  {{source('tenpo_recarga_facil','product')}} prod ON pope.product_id = prod.id
JOIN
  {{source('tenpo_recarga_facil','operator')}} ope ON ope.id = pope.operator_id