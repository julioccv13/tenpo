{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

select
    date_trunc(date(trx_timestamp), month) as mes
    ,CASE 
      when descripcion_sia IN ('DENEGADA','IMPORTESUPERALIMITE', 'APROBADA') THEN descripcion_sia
      else 'OTROSRECHAZOS'
      END AS detalle
    ,count(distinct user) as cant_usr
    ,count(distinct id) as cant_trx
    ,cast(sum(monto) as numeric) as mto_total
    ,CASE 
      WHEN f_aprobada IS TRUE then "aprobadas"
      ELSE "rechazadas"
    end as status
from {{ref('notifications_history')}}
where  not tipo_tx = 'devolución'
group by mes,detalle, status
order by mes desc, status,detalle