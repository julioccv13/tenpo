{{ config(materialized='table') }}
with trx_exitosas_rf as (
  select distinct
    correo_usuario as correo_usuario, -- Este correo ya viene hasheado de funnel_recargas
    id_trx,
    id_sus,
    fecha,
    monto,
    date_diff(date(fecha), date(lag(fecha) over (partition by correo_usuario order by fecha)), DAY) as dias_trx,
    round(if(not medio_pago='Puntos RecargaFácil', monto*0.02,0),0) as puntos_ganados_trx,
    if(not medio_pago='Puntos RecargaFácil', sum(if(not medio_pago='Puntos RecargaFácil', monto*0.02,0)) over (partition by correo_usuario order by fecha),0) as puntos_ganados_hist,
    if(medio_pago='Puntos RecargaFácil', monto,0) as puntos_usados,
    medio_pago, 
  from {{ref('funnel_recargas')}}
  where trx_estado = '5. Finalizado'
    and origen = 'RECARGAFACIL'
    and correo_usuario != ''
),
trx as(
  select 
    *,
    sum(if(dias_trx<=45,0,1)) over (partition by correo_usuario order by fecha) as ciclo,
  from trx_exitosas_rf
),
puntos_vigentes as (
  select 
    *,
    sum(puntos_ganados_trx-puntos_usados) over (partition by correo_usuario,ciclo order by fecha) as vigentes,
  from trx
)
select 
  *,
  if(ciclo=lag(ciclo) over (partition by correo_usuario order by fecha),0,lag(vigentes) over (partition by correo_usuario order by fecha)) as perdidos
from puntos_vigentes
order by correo_usuario, fecha