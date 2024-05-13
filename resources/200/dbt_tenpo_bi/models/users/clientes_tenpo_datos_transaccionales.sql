{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table' 
  ) 
}}

with transacciones as (
    select 
      *
    from {{ref("economics")}} e
      join {{ref("users_tenpo")}} u on (e.user=u.id)
), dias_entre_transacciones as (
    select  
    *,
    date_diff(
        fecha,
        lag(fecha) 
        OVER (PARTITION BY linea,user ORDER BY fecha),  DAY) 
        as dias_entre_trx
    from transacciones
)

select distinct 
    user,
    linea,
    count(distinct linea) over (partition by user) as dist_linea,
    max(fecha) as last_trx,
    sum(monto) as monto_total,
    count(distinct trx_id) as cant_trx,
    count(distinct comercio) as dist_comercios,
    avg(dias_entre_trx) as mtbt,
    date_diff(min(fecha),date(ob_completed_at), day) as days_to_activate
from dias_entre_transacciones
group by user,linea,ob_completed_at