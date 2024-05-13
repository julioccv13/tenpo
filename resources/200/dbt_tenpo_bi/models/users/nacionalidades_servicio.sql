{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table'
  ) 
}}

select distinct
  nacionalidad,
  fecha,
  id_usuario,
  linea,
  t1.nombre,
  sum(monto) as monto,
  count(distinct trx_id) as cant_trx
from {{ref("economics")}}  t1
  join {{ref("demographics")}} t2 on t1.user=t2.id_usuario 
group by 
  nacionalidad,
  fecha,
  id_usuario,
  linea,
  t1.nombre