{{ 
    config(
        materialized='table'
        ,tags=["daily", "bi", "datamart"]
        ,project=env_var('DBT_PROJECT19', 'tenpo-datalake-sandbox')
    )
}}
with usuarios_rf as (
  select distinct 
    trx.correo_usuario as email,
    rec.suscriptor,
    trx.fecha_creacion,
    date_diff(current_date(),date(trx.fecha_creacion),day) as recency
  FROM
    {{source("topups_web","ref_transaccion")}} trx
  JOIN
    {{source("topups_web","ref_recarga")}} rec ON trx.id=rec.id_transaccion
  join {{source("topups_web","ref_producto")}} p on (rec.id_producto= p.id)
  left join {{source("tenpo_users","users")}} u on (trx.correo_usuario = u.email)
  WHERE true
    and trx.id_origen IN (1,2,5) 
    and trx.id_estado = 20 
    AND rec.id_estado = 27
    and p.id_tipo_producto = 1
    and date_diff(current_date(),date(trx.fecha_creacion),day) <=360
    and u.id is null
  qualify row_number() over (partition by email order by fecha_creacion desc) = 1
)
select distinct  
  usuarios_rf.*,
  case 
    when recency <=30 then '<=30'
    when recency <=90 then '31 a 90'
    when recency <=180 then '91 a 180'
    when recency <=360 then '181 a 360'
    else ">360"
  end as cat_recency
from usuarios_rf
  left join {{source("tenpo_users","users")}} using (email)
where id is null
and email <> ""
qualify row_number() over (partition by cat_recency order by rand()) <=60000