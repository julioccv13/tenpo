{{ config(materialized='table',  tags=["daily", "bi"]) }}
  with datos_topups as (
    SELECT DISTINCT
      {{ hash_sensible_data('t.correo_usuario') }} as correo_usuario,
      user,
      MAX(DATE(t.fecha_creacion)) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_order_date,
      DATE_DIFF(CURRENT_DATE(),MAX(DATE(t.fecha_creacion)) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) as recency,
      AVG(t.monto) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as avg_amount,
      COUNT(1) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as count_trx,
      LAST_VALUE(
        CASE
          WHEN t.id_usuario  > 0 THEN 'Registrado'
          WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN 'Semiregistrado'
          ELSE 'Anónimo' 
          END
        ) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        AS tipo_usuario,

      LAST_VALUE(tp.nombre) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as categoria,
      LAST_VALUE(o.nombre) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as operador,
      LAST_VALUE(IF(users.email IS NULL, 0, 1)) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  as en_tenpo,
      IF(LAST_VALUE(last_ndd_service_bm) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)="Topups",1,0)  as migrado_tu,
    FROM {{source("topups_web","ref_transaccion")}}  t
        INNER JOIN {{source("topups_web","ref_recarga")}} r ON t.id = r.id_transaccion
        INNER JOIN {{source('topups_web','ref_producto')}} p ON p.id = r.id_producto 
        INNER JOIN {{source('topups_web','ref_operador')}} o ON o.id = p.id_operador
        INNER JOIN {{source('topups_web','ref_comisiones')}} c ON c.id_producto = p.id 
        INNER JOIN {{source('topups_web','ref_tipo_producto')}} tp ON tp.id = p.id_tipo_producto
        --LEFT JOIN (SELECT DISTINCT email FROM {{source('tenpo_users','users')}}where state IN (4,7,8,21,22)) users on users.email = t.correo_usuario
        LEFT JOIN (SELECT DISTINCT email, last_ndd_service_bm,id as user FROM {{ref('users_allservices')}} where state IN (4,7,8,21,22)) users on users.email = t.correo_usuario
    WHERE t.id_origen IN (1,2,5) AND t.id_estado = 20 AND r.id_estado = 27 and t.correo_usuario <> ""
    
  )
  select distinct *,
     ntile(5) over (partition by categoria order by last_order_date) as rfm_recency,
     ntile(5) over (partition by categoria order by count_trx) as rfm_frequency,
     ntile(5) over (partition by categoria order by avg_amount) as rfm_monetary
    from 
      datos_topups