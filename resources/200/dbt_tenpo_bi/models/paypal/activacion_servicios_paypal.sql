{{ config(materialized='table') }}

with primera_plataforma as (
  select distinct
    id_cliente,
    min(if (tip_trx = 'RETIRO_APP_PAYPAL', id_cuenta,null)) as id_cuenta,
    min(if (tip_trx = 'RETIRO_APP_PAYPAL', fecha_ingreso_cuenta,null)) as fecha_ingreso_cuenta_app,
    min(if (not tip_trx = 'RETIRO_APP_PAYPAL', fecha_ingreso_cuenta,null)) as fecha_ingreso_cuenta_web,
  from `tenpo-bi-prod.paypal.transacciones_paypal`  
  where true
  group by id_cliente
),
usuarios_preweb as (
  select distinct
    id_cuenta,
    true as preweb
  from primera_plataforma
  where fecha_ingreso_cuenta_web<fecha_ingreso_cuenta_app
)

select distinct
  count(distinct if(preweb,id_cuenta,null)) 
    over (partition by tip_trx,tipo_usuario,
      date_trunc(date(fecha_ingreso_cuenta), MONTH)) 
    as cuentas_preweb,
  count(distinct id_cuenta) 
    over (partition by tip_trx,tipo_usuario,
      date_trunc(date(fecha_ingreso_cuenta), MONTH)) 
    as cuentas_creadas,
  count(distinct id_cuenta) 
    over (partition by tip_trx,tipo_usuario,
      date_trunc(date(fecha_ingreso_cuenta), MONTH),
      date_trunc(date(fecha_trx), MONTH)) 
    as cuentas_activas,
  date_trunc(date(fecha_ingreso_cuenta), MONTH) as mes_creacion_cuenta,
  date_trunc(date(fecha_trx), MONTH) as mes_trx,
  tip_trx,
  tipo_usuario,
  date_diff(date_trunc(date(fecha_trx), MONTH),date_trunc(date(fecha_ingreso_cuenta), MONTH), MONTH) as mes_n,
from {{ref('transacciones_paypal')}} 
  left join usuarios_preweb using (id_cuenta)
where tip_trx is not null
order by tip_trx,tipo_usuario,mes_creacion_cuenta,mes_trx
