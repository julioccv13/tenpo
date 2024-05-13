SELECT
        (DATE(r.fecha_recarga)) AS dia,
        FORMAT_DATE('%Y-%m-01', DATE(r.fecha_recarga)) mes,
        SUM(t.monto) as gpv_clp,
        SUM(t.monto / 680) as gpv_usd,
        COUNT(1) as trx,
#       COUNT(DISTINCT IF(t.id_usuario  > 0, CAST (t.id_usuario AS STRING), (IF(t.id_usuario < 0 AND t.correo_usuario <> "",t.correo_usuario,r.suscriptor)))) AS usr,
#       MD5(IF(t.id_usuario  > 0, CAST (t.id_usuario AS STRING), (IF(t.id_usuario < 0 AND t.correo_usuario <> "",t.correo_usuario,r.suscriptor)))) as usr,
        MD5(r.suscriptor) as usr,
         CASE 
          WHEN t.id_usuario  > 0 THEN 'Registrado'
          WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN 'Semiregistrado'
          ELSE 'Anónimo' 
          END
          AS tipo_usuario,
          producto_recarga,
          tp.codigo
      FROM {{source("topups_web","ref_transaccion")}}  t
      INNER JOIN {{source("topups_web","ref_recarga")}} r ON t.id = r.id_transaccion
      INNER JOIN {{source('topups_web','ref_producto')}} p ON p.id = r.id_producto 
      INNER JOIN {{source('topups_web','ref_operador')}} o ON o.id = p.id_operador
      INNER JOIN {{source('topups_web','ref_comisiones')}} c ON c.id_producto = p.id 
      INNER JOIN {{source('topups_web','ref_tipo_producto')}} tp ON tp.id = p.id_tipo_producto 
      WHERE t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5)
      GROUP BY mes, tipo_usuario, dia, producto_recarga, tp.codigo, usr
      order by 1 asc