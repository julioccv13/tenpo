{{ config(materialized='table') }}

with 
  puntos_registrados as (
  SELECT  
    saldo,
    {{ hash_sensible_data('per.correo') }} as correo,
    "Registrados" as tipo
  FROM {{source('topups_web','slp_puntos_rf')}} p 
    join {{source('topups_web','slp_usuario')}} u on u.id=p.id_usuario
    join {{source('topups_web','bof_persona')}} per on per.id_persona= u.id_persona
  where saldo>0
  ),
  puntos_semiregistrados as (
    select distinct
      sum(r.puntos) over (partition by t.correo_usuario) as saldo,
      t.correo_usuario as correo,
      "Semi-registrados" as tipo
    from {{source("topups_web","ref_transaccion")}} t
      join {{source("topups_web","ref_recarga")}} r on t.id=r.id_transaccion 
    where true
      and t.id_estado =20 
      and r.id_estado =27 
      and cast(t.fecha_creacion as date) >= date_sub(current_date(), INTERVAL 45 DAY)
      and r.puntos > 0
      and t.correo_usuario not in (
        select 
          {{ hash_sensible_data('correo') }} as correo
        from {{source('topups_web','bof_persona')}}
        )
  )
  select * from puntos_registrados
  UNION ALL
  select * from puntos_semiregistrados 