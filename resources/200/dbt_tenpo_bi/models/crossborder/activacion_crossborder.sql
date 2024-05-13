{{ config(materialized='table') }}

with data_senders as (
  select
    count(distinct id)
    over (partition by 
        date_trunc(date(created_at), MONTH)) 
    as cuentas_creadas,
    date_trunc(date(created_at), MONTH) as mes_creacion_cuenta
  from {{ref('senders_crossborder')}}
),
data_trx as (
  select distinct
    count(distinct user_id) 
      over (partition by 
          date_trunc(date(sender_created_at), MONTH),
          date_trunc(date(date_trx_created_at), MONTH)) 
      as cuentas_activas,
    date_trunc(date(sender_created_at), MONTH) as mes_creacion_cuenta,
    date_trunc(date(date_trx_created_at), MONTH) as mes_trx,
    date_diff(date_trunc(date(date_trx_created_at), MONTH),date_trunc(date(sender_created_at), MONTH), MONTH) as mes_n,
  from {{ref('transactions_crossborder')}} t
  where 
    date(sender_created_at) <=date_trx_created_at
    and sender_created_at is not null
  order by mes_creacion_cuenta,mes_trx
)
select distinct
  *
from data_trx t
  right join data_senders s using (mes_creacion_cuenta)