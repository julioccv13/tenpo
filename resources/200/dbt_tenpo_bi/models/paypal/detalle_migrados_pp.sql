{{ config(materialized='table',  tags=["daily", "bi"]) }}
  with datos_paypal as (
    with 
      --- Usuarios Tenpo
      users as (
        SELECT DISTINCT 
          email,
          tributary_identifier as rut,
          last_ndd_service_bm 
        FROM {{ref('users_allservices')}} 
        where state IN (4,7,8,21,22)
      ),
      --- Clientes Paypal
      clientes as (
        select distinct
          last_value(id) OVER (PARTITION BY rut ORDER BY fec_hora_ingreso ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as id_cliente,
          last_value(fec_hora_ingreso) OVER (PARTITION BY rut ORDER BY fec_hora_ingreso ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fec_creacion,
          rut 
        from {{source('paypal','pay_cliente')}}
        where tipo_usuario="PERSONA"
      ),
      ---Cuentas Paypal
      cuentas as (
        select distinct
          id as id_cuenta,
          last_value(id_cliente) OVER (PARTITION BY id ORDER BY fec_fecha_hora_ingreso ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as id_cliente,
          last_value(tip_operacion_multicaja) OVER (PARTITION BY id ORDER BY fec_fecha_hora_ingreso ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as tip_trx,
        from {{source('paypal','pay_cuenta')}}
      ),
      ---Datos transaccionales por cuenta paypal
      datos_trx as (
        select distinct
          id_cuenta,
          last_value(fec_fechahora_envio) OVER (PARTITION BY id_cuenta ORDER BY fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_order_date,
          timestamp_diff(CAST(CURRENT_DATE() AS TIMESTAMP),last_value(fec_fechahora_envio) OVER (PARTITION BY id_cuenta ORDER BY fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),DAY) as recency,
          count(distinct id) OVER (PARTITION BY id_cuenta) as count_trx,
          avg(mto_monto_dolar) OVER (PARTITION BY id_cuenta) as avg_amount,
        from {{source('paypal','pay_transaccion')}}
        WHERE est_estado_trx IN (2,3,8,17,24)
          AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL','RETIRO_APP_PAYPAL')
      )
      select distinct
        *,
        IF(LAST_VALUE(last_ndd_service_bm) OVER (PARTITION BY id_cuenta ORDER BY last_order_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) IS NOT NULL,1,0)  as en_tenpo,
        IF(LAST_VALUE(last_ndd_service_bm) OVER (PARTITION BY id_cuenta ORDER BY last_order_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)="Paypal",1,0)  as migrado_pp,
      from clientes 
      join cuentas USING (id_cliente)
      left join datos_trx USING (id_cuenta)
      left join users  USING (rut)
  )
  select distinct *,
    IF (last_order_date is NULL,0, ntile(5) over (partition by tip_trx,(CASE WHEN last_order_date IS NOT NULL THEN 'NOTNULL' ELSE 'NULL' END) order by last_order_date)) as rfm_recency,
    IF (count_trx is NULL,0, ntile(5) over (partition by tip_trx,(CASE WHEN count_trx IS NOT NULL THEN 'NOTNULL' ELSE 'NULL' END) order by count_trx)) as rfm_frequency,
    IF (avg_amount is NULL,0, ntile(5) over (partition by tip_trx,(CASE WHEN avg_amount IS NOT NULL THEN 'NOTNULL' ELSE 'NULL' END) order by avg_amount)) as rfm_monetary
  from 
    datos_paypal