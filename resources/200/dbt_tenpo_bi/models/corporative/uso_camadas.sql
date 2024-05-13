{{ config(materialized='table',tags=["daily", "bi"]) }}

with onboardings as (
    select distinct
        id as user,
        ob_completed_at
    from {{ref('users_tenpo')}}
),
datos as (
    select distinct
    count(distinct user) 
        over (partition by 
            date_trunc(date(ob_completed_at), MONTH)) 
        as cant_usr_camada,
    count(distinct user) 
        over (partition by 
            date_trunc(date(ob_completed_at), MONTH),
            date_trunc(date(fecha), MONTH)) 
        as mau,
    count(distinct trx_id) 
        over (partition by 
            date_trunc(date(ob_completed_at), MONTH),
            date_trunc(date(fecha), MONTH)) 
        as trx,
    sum(distinct monto) 
        over (partition by 
            date_trunc(date(ob_completed_at), MONTH),
            date_trunc(date(fecha), MONTH)) 
        as mto,
    date_trunc(date(ob_completed_at), MONTH) as mes_ob_completed_at,
    date_trunc(date(fecha), MONTH) as mes_trx,
    date_diff(date_trunc(date(fecha), MONTH),date_trunc(date(ob_completed_at), MONTH), MONTH) as mes_n,
    from {{ref('economics')}} t
        join onboardings using (user)
    where true
        and date(ob_completed_at)<=fecha
        and date(ob_completed_at)>= (select min(fecha) from {{ref('economics')}})
        and linea not in ('cash_in','cash_out','aum_savings','aum_tyba','reward','p2p_received')
        and nombre not like "%Devolución%"
        and date_trunc(fecha,month) < (select max(date_trunc(fecha,month)) as mes_max from {{ref('economics')}})
)
select 
    *,
    safe_divide(trx, mau) as trx_mau,
    safe_divide(mto, mau) as mto_mau,
    safe_divide(mto, trx) as mto_trx,
from datos