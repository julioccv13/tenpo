{{ config(materialized='table',  tags=["hourly", "bi"]) }}
with 
  --- Clientes Paypal
  clientes as (
    select distinct
      id as id_cliente,
      fec_hora_ingreso as fec_creacion,
      last_value(tipo_usuario) over (partition by id order by fec_hora_ingreso) as tipo_usuario,
    from {{source('paypal','pay_cliente')}}
  ),
  ---Cuentas Paypal
  cuentas as (
    select distinct
      id as id_cuenta,
      id_cliente,
      fec_fecha_hora_ingreso as fec_creacion_cuenta,
      tip_operacion_multicaja as tip_trx,
    from {{source('paypal','pay_cuenta')}}
  ),
  ---Datos transaccionales por cuenta paypal
  datos_trx as (
    select distinct
      id_cuenta,
      first_value(fec_fechahora_envio) OVER (PARTITION BY id_cuenta,tip_trx ORDER BY fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fec_pri_trx_exitosa,
      last_value(fec_fechahora_envio) OVER (PARTITION BY id_cuenta,tip_trx ORDER BY fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fec_ult_trx_exitosa,
      count(distinct id) OVER (PARTITION BY id_cuenta,tip_trx) as cant_trx,
      avg(dias_entre_trx) OVER (PARTITION BY id_cuenta,tip_trx ORDER BY fec_fechahora_envio RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as dias_prom_entre_trx,
    from {{source('paypal','pay_transaccion')}}
      left join (
        select distinct
          id_cuenta,
          timestamp_diff(lead(fec_fechahora_envio) OVER (PARTITION BY id_cuenta ORDER BY fec_fechahora_envio), fec_fechahora_envio,  DAY) as dias_entre_trx,
        FROM {{source('paypal','pay_transaccion')}}
        WHERE est_estado_trx IN (2,3,8,17,24)
          AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL','RETIRO_APP_PAYPAL')
      ) USING (id_cuenta)
    WHERE est_estado_trx IN (2,3,8,17,24)
      AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL','RETIRO_APP_PAYPAL')
  )
---Datos agregados
select 
  tipo_usuario,
  id_cliente,
  fec_creacion, 
  id_cuenta,
  fec_creacion_cuenta,
  tip_trx,
  IF(fec_creacion<timestamp("2018-01-01"), null, fec_pri_trx_exitosa) as fec_pri_trx_exitosa,
  count(distinct id_cuenta) over (partition by date_trunc(date(fec_creacion_cuenta),month)) as cant_cuenta_mensual,
  fec_ult_trx_exitosa,
  cant_trx,
  dias_prom_entre_trx,
  timestamp_diff(fec_creacion_cuenta, fec_creacion, DAY) as dias_activacion_cuenta,
  timestamp_diff(IF(fec_creacion<timestamp("2018-01-01"), null, fec_pri_trx_exitosa), fec_creacion, DAY) as dias_primera_trx,
  timestamp_diff(CURRENT_TIMESTAMP(), fec_ult_trx_exitosa,  DAY) as dias_ultima_trx,
from clientes 
  join cuentas USING (id_cliente)
  left join datos_trx USING (id_cuenta)