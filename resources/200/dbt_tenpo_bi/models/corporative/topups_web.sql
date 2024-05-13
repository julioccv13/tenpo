{{ config(materialized='ephemeral',  tags=["daily", "bi"]) }}

SELECT
   DATE(t.fecha_creacion , "America/Santiago") AS dia,
   DATE_TRUNC(DATE(t.fecha_creacion , "America/Santiago"), ISOWEEK) semana,
   DATE_TRUNC(DATE(t.fecha_creacion  ,"America/Santiago"), MONTH) mes,
   t.monto gpv_clp,
   t.monto / valor_dolar_cierre as gpv_usd,
   CAST(t.id as STRING) as trx,
   CASE 
    WHEN t.id_usuario  > 0 THEN  CAST (t.id_usuario AS STRING)
    WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN t.correo_usuario
    ELSE CAST(r.suscriptor  as STRING)
    END AS usr,
   'top_ups' as linea,
   'web' as canal,
   'generico' as tipo_usuario,
FROM (select distinct * from {{source('topups_web','ref_transaccion')}})  t
JOIN (select distinct * from {{source('topups_web','ref_recarga')}}) r ON t.id = r.id_transaccion
JOIN (select distinct * from {{source('topups_web','ref_producto')}}) p ON p.id = r.id_producto 
JOIN (select distinct * from {{source('topups_web','ref_operador')}}) o ON o.id = p.id_operador
JOIN (select distinct * from {{source('topups_web','ref_comisiones')}}) c ON c.id_producto = p.id 
JOIN (select distinct * from {{source('topups_web','ref_tipo_producto')}}) tp ON tp.id = p.id_tipo_producto
JOIN (select distinct * from {{ ref('dolar') }})   ON DATE( t.fecha_creacion , "America/Santiago") = fecha
WHERE 
  t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5)
QUALIFY 
    row_number() over (partition by CAST(t.id AS STRING) order by t.fecha_actualizacion desc) = 1