{{ 
  config(
    materialized='table', 
  ) 
}}

with caracterizacion_trx as (
    SELECT
    a.*
    ,case when banco in ('BANCO SECURITY', 'BANCO BICE', 'ITAÚ CORPBANC', 'SCOTIABANK/DESARROLLO') then 3 # 'Bancarizado Premium'
    when banco = 'BANCO ESTADO' then 1 # 'Cuenta RUT'
    else 2 # Medium
    end as caracterizacion_int
    from {{ ref('bank_accounts_per_user') }} a
    left join {{ ref('users_tenpo') }} b
    on a.user = b.id
), caracterizacion_user as (
    select
    user
    ,max(caracterizacion_int) as caracterizacion_int
    from caracterizacion_trx
    group by 1
)

select
    a.id as user
    ,date_trunc(date(ob_completed_at, 'America/Santiago'), month) as mes_ob
    ,caracterizacion_int
    ,case when caracterizacion_int = 3 then '3. Large'
            when caracterizacion_int = 2 then '2. Medium'
            when caracterizacion_int = 1 then '1. Small'
            else '0. Sin cashin reconocible (o solo fisico)'
            end as caracterizacion
from {{ ref('users_tenpo') }} a
left join caracterizacion_user b
on a.id = b.user
where a.ob_completed_at is not null
and a.state in (4,7,8)
qualify row_number() over (partition by a.id order by updated_at desc) = 1
