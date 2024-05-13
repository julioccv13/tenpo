{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"]
  ) 
}}

with 
  tf_act as (
    select distinct
      count(distinct tj.id) over (partition by date_trunc(date(tj.fecha_creacion),month)) as cant_tf_activas,
      date_trunc(date(tj.fecha_creacion),month) as mes,
    from {{source('prepago','prp_tarjeta')}} tj
    where tj.tipo = 'PHYSICAL'
      and tj.estado = 'ACTIVE'
  ),
  tf_act_acum as (
    select distinct
      *,
      sum(cant_tf_activas) over (order by mes rows between unbounded preceding and current row) as cant_tf_act_acum
    from tf_act 
  ),
  tf_usada as (
    select 
      count(distinct  t1.id_tarjeta) as cant_tarjetas_con_compras, 
      count(distinct  if(not es_comercio_presencial,t1.id_tarjeta,null)) as cant_tarjetas_con_compras_virtuales,
      count(distinct  if(es_comercio_presencial,t1.id_tarjeta,null)) as cant_tarjetas_con_compras_fisicas,
      date_trunc(date(t1.fecha_creacion),month) as mes,
      from {{source('prepago','prp_movimiento')}} t1
      right join {{source('prepago','prp_tarjeta')}} t2 on (cast(t1.id_tarjeta as NUMERIC)=cast(t2.id as NUMERIC))
    where t2.tipo = 'PHYSICAL'
      and t2.estado = 'ACTIVE'
      AND tipofac in (3006,3007,3028,3029,3009, 3031, 5, 3010,3011,3012,3030) 
      AND (t1.estado  in ('PROCESS_OK','AUTHORIZED') 
      OR (t1.estado = 'NOTIFIED' AND DATE(t1.fecha_creacion  , "America/Santiago") BETWEEN DATE_SUB(CURRENT_DATE("America/Santiago"), INTERVAL 3 DAY) AND CURRENT_DATE("America/Santiago")))
      AND indnorcor  = 0
    group by mes
  )
select 
  *, 
  SAFE_DIVIDE(cant_tarjetas_con_compras,cant_tf_act_acum) AS ratio_cant_tarjetas_con_compras,
  SAFE_DIVIDE(cant_tarjetas_con_compras_virtuales,cant_tf_act_acum) AS ratio_cant_tarjetas_con_compras_virtuales,
  SAFE_DIVIDE(cant_tarjetas_con_compras_fisicas,cant_tf_act_acum) AS ratio_cant_tarjetas_con_compras_fisicas,
from tf_act_acum 
  join tf_usada using (mes)