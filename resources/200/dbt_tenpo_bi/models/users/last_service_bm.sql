{{ config(materialized='table') }}

with datos as(
  with pp_cli as (
          SELECT DISTINCT
          pp_cli.rut,
          pp_cli.fec_ultimo_login_exitoso,
          pp_trx.fec_fechahora_envio,
          pp_cli.fec_hora_ingreso,
          LAST_VALUE(pp_cli.ultimo_servicio) OVER (PARTITION BY pp_cli.rut order by pp_cli.fec_ultimo_login_exitoso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as last_serv_paypal_bm,
          LAST_VALUE(
            IF(
              pp_cli.fec_ultimo_login_exitoso < t_usr.created_at,
              pp_cli.fec_ultimo_login_exitoso,
              null
            )
          ) 
          OVER (PARTITION BY pp_cli.rut ORDER BY pp_cli.fec_ultimo_login_exitoso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) 
          AS last_lpp_bm,
          LAST_VALUE(
            IF(
              fec_fechahora_envio < t_usr.created_at,
              fec_fechahora_envio ,
              null
            )
          ) 
          OVER (PARTITION BY pp_cli.rut ORDER BY fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) 
          AS last_tpp_bm,
        LAST_VALUE(
            IF(
              pp_cli.fec_hora_ingreso <t_usr.created_at,
              pp_cli.fec_hora_ingreso ,
              null
            )
          ) 
          OVER (PARTITION BY pp_cli.rut ORDER BY pp_cli.fec_hora_ingreso RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) 
          AS pp_created_at,
          FROM
            {{ source('tenpo_users', 'users') }} as t_usr
          LEFT JOIN
            {{ source('paypal', 'pay_cliente') }} pp_cli ON pp_cli.rut=t_usr.tributary_identifier
          JOIN
            {{ source('paypal', 'pay_cuenta') }} pp_acc ON pp_cli.id = pp_acc.id_cliente
          LEFT JOIN
            {{ source('paypal', 'pay_transaccion') }} pp_trx ON pp_trx.id_cuenta = pp_acc.id 
          WHERE
            pp_acc.tip_operacion_multicaja = pp_cli.ultimo_servicio
  ),
  tu_trx as (
        SELECT DISTINCT
          t.correo_usuario,
          t.fecha_creacion,
          LAST_VALUE(tp.nombre) OVER (PARTITION BY t.correo_usuario order by t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as categoria_bm,
        FROM {{ source('topups_web', 'ref_transaccion') }}  t
          LEFT JOIN {{ source('topups_web', 'bof_persona') }} bp ON t.id_usuario = bp.id_persona 
          INNER JOIN {{ source('topups_web', 'ref_recarga') }} r ON t.id = r.id_transaccion
          INNER JOIN {{ source('topups_web', 'ref_producto') }} p ON p.id = r.id_producto 
          INNER JOIN {{ source('topups_web', 'ref_operador') }} o ON o.id = p.id_operador
          INNER JOIN {{ source('topups_web', 'ref_comisiones') }} c ON c.id_producto = p.id 
        INNER JOIN {{ source('topups_web', 'ref_tipo_producto') }} tp ON tp.id = p.id_tipo_producto 
        WHERE t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5) AND t.correo_usuario <> "" 
    )
  
      SELECT DISTINCT
        t_usr.id,
        t_usr.tributary_identifier,
        FIRST_VALUE(tu_trx.fecha_creacion) OVER (PARTITION BY tu_trx.correo_usuario ORDER BY tu_trx.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as last_tu_bm,
        last_lpp_bm,
        last_tpp_bm,
        pp_created_at,
        t_usr.created_at,
        last_serv_paypal_bm,
        categoria_bm,
      FROM 
        {{ source('tenpo_users', 'users') }} as t_usr
      LEFT JOIN
          pp_cli
        ON pp_cli.rut=t_usr.tributary_identifier
      left join
        tu_trx
        ON tu_trx.correo_usuario = t_usr.email
        AND t_usr.created_at >= tu_trx.fecha_creacion     
  )
  select 
  *,
  IF(timestamp_diff(current_timestamp(),last_tpp_bm, DAY)>90 OR last_tpp_bm is null,"Inactivos - PP","Activos - PP") as actividad_pp_bm,
  IF(timestamp_diff(current_timestamp(),last_tu_bm, DAY)>90,"Inactivos - TU","Activos - TU") as actividad_tu_bm,
  
  CASE 
    WHEN last_lpp_bm IS NULL AND last_tpp_bm IS NULL THEN pp_created_at
    WHEN (last_lpp_bm IS NULL AND last_tpp_bm IS NOT NULL) OR (last_lpp_bm<last_tpp_bm) THEN last_tpp_bm
    WHEN (last_lpp_bm IS NOT NULL AND last_tpp_bm IS NULL) OR (last_lpp_bm>last_tpp_bm) THEN last_lpp_bm
    END AS last_pp_use
  from datos
