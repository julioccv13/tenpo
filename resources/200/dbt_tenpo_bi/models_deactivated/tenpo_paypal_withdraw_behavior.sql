{{ config(materialized='table',  tags=["hourly", "bi"]) }}

with datos as (
  with
    usuarios_paypal_app as(
      select distinct
        user,
        first_value(creacion) OVER (partition by user order by creacion RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_primer_retiro,
        first_value(trx_id) OVER (partition by user order by fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as id_trx_primer_retiro,
      from {{ref("economics")}} trx
      where trx.linea="paypal"
    )

  select distinct
    *,
    CASE
      WHEN id_trx_primer_retiro = trx_id THEN "Primer Retiro"
      WHEN trx.fecha < fecha_primer_retiro THEN "Antes"
      ELSE "Después" 
      END as pre_post_retiro_paypal
  from {{ref("economics")}} trx
  JOIN usuarios_paypal_app USING (user)
  )
  
  select distinct
    *,
    CASE 
      WHEN pre_post_retiro_paypal ="Antes" THEN sum(1) OVER (PARTITION BY user, pre_post_retiro_paypal ORDER BY user, pre_post_retiro_paypal,fecha desc,creacion desc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      WHEN pre_post_retiro_paypal ="Después" THEN sum(1) OVER (PARTITION BY user, pre_post_retiro_paypal ORDER BY user, pre_post_retiro_paypal,fecha,creacion RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      WHEN pre_post_retiro_paypal ="Primer Retiro" THEN 0
      END as orden
  from datos
  order by 1,2,4