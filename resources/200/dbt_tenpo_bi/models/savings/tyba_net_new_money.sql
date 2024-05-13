{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

select
  fecha,
  cast(sum(if(linea='investment_tyba',monto,0)) as int64) as investment,
  cast(sum(if(linea='withdrawal_tyba',monto,0)) as int64)*-1 as withdrawal,
  cast(sum(if(linea='investment_tyba',monto,0)) as int64)-cast(sum(if(linea='withdrawal_tyba',monto,0)) as int64) as net_new_money
from {{ref('economics')}}
where linea in ('investment_tyba','withdrawal_tyba')
group by fecha
order by fecha