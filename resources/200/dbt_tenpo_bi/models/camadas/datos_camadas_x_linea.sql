{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

with
users as (
  select distinct
    id as user,
    date_trunc(date(ob_completed_at),month) as mes_ob
  from {{ref('users_tenpo')}}
),
economics_user_mensual as (
  select distinct
    user,
    linea,
    date_trunc(fecha,month) as mes_trx,
    count(distinct nombre) as cant_nombre,
    count(distinct comercio) as cant_comercio,
    count(distinct trx_id) as cant_trx,
    sum(monto) as monto,
  from {{ref('economics')}}
  group by 1,2,3
),
base as (
  select distinct 
    mes_ob,
    date_diff(mes_trx,mes_ob,month) as mes_trx_desde_ob,
    linea,
    count(distinct user) as cant_usuarios,
    avg(cant_nombre) as avg_mensual_cant_nombre,
    avg(cant_comercio) as avg_mensual_cant_comercio,
    sum(monto) as total_linea,
    sum(cant_trx) as total_trx
  from economics_user_mensual e
    join users u using (user)
  where true
    and date_diff(mes_trx,mes_ob,month)>=0
    and mes_ob is not null
  group by 1,2,3
)
select distinct 
  *
from base