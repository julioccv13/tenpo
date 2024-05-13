{{ config(materialized='ephemeral') }}
with 
datos as (
  with 
  correos_trx as (
    SELECT DISTINCT
      rut,
      trx.cor_mail_envio,
      id_cuenta 
    FROM
      {{source('paypal','pay_transaccion')}} as trx
    JOIN
      (
        select
          t2.id as id_cuenta, 
          rut 
        from {{source('paypal','pay_cliente')}} as t1
        JOIN 
        {{source('paypal','pay_cuenta')}} as t2
        ON t1.id = t2.id_cliente 
      )
      using (id_cuenta)
    ORDER BY 1
  ),
  correos_cuenta as (
    select distinct
      rut,
      t2.correo_cuenta,
      t2.id as id_cuenta
    from {{source('paypal','pay_cliente')}}as t1
    JOIN 
    {{source('paypal','pay_cuenta')}} as t2
    ON t1.id = t2.id_cliente
  ),
  correos_cliente as (
     select distinct
      rut,
      t1.correo_usuario,
      t2.id as id_cuenta
    from {{source('paypal','pay_cliente')}}as t1
    LEFT JOIN 
    {{source('paypal','pay_cuenta')}} as t2
    ON t1.id = t2.id_cliente
  )
  select distinct * from (
    select * from correos_cuenta 
    union all
    select * from correos_trx 
    union all
    select * from correos_cliente 
  )
)
select * from datos