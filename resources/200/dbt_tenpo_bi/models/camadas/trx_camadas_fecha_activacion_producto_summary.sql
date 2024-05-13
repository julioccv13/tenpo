{{ 
  config(
    tags=["daily", "bi"],
    materialized='view', 
  ) 
}}

with datas as (
    select 
    mes_analisis
    ,linea
    ,mes_proximo
    ,diferencia_meses
    ,count(distinct user) as users_unicos
    from {{ref('trx_camadas_fecha_activacion_producto')}}
    group by 1,2,3,4
), rolling_data as (
    select 
        datas.*
        ,max(users_unicos) over (partition by mes_analisis, linea) as max_users
        ,users_unicos/max(users_unicos) over (partition by mes_analisis, linea) as pct_users_iniciales
    from datas
)

select *
from rolling_data