{{ config(materialized='table') }}

with 
  usr as (
    select distinct 
      id, 
      DATE(ob_completed_at,"America/Santiago") AS ob_completed_at
    from {{ source('tenpo_users', 'users') }}),
  eco_modificado as (
    select distinct
      * except (linea), 
      CASE 
        WHEN linea = 'cash_out' and nombre = 'Cashout físico' THEN 'physical_cashout'
        WHEN linea = 'cash_in_savings' and nombre = 'Aporte Regla' THEN 'cash_in_savings_rule'
        ELSE linea END 
        as linea
    from {{ ref('economics') }}
  )
  select distinct
    eco.user,
    eco.linea,
    eco.fecha,
    eco.trx_id,
    eco.monto,
    uall.last_ndd_service_bm as fuente,
      IF( first_value(date_trunc(fecha,month)) OVER (partition by eco.user,eco.linea order by eco.fecha ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
          = date_trunc(fecha, month), 1, 0) as primera_vez_mes,
      IF(
      first_value(fecha) OVER (partition by eco.user,eco.linea order by eco.fecha ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        = fecha, 1, 0) as primera_vez_dia,
    date_trunc(DATE(ob_completed_at),month) as mes_ob,
  from eco_modificado as eco
    join usr on (eco.user = usr.id)
    join {{ ref('users_allservices') }} as uall on (eco.user=uall.id)

