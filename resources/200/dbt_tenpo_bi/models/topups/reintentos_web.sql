{{ config(materialized='table',  tags=["daily", "bi"]) }}

with 
  exitosas as (
    select distinct
      t.id,
      t.fecha_creacion AS t_actual,
      r.suscriptor,
      LAG(t.fecha_creacion) OVER (PARTITION BY r.suscriptor ORDER BY t.fecha_creacion) AS t_anterior,
    from {{source("topups_web","ref_transaccion")}} t
      join {{source("topups_web","ref_recarga")}} r on (t.id=r.id_transaccion)
    where true
      and t.id_estado=20 AND r.id_estado=27
    order by t.fecha_creacion desc
  ),
  fallidas as (
    select distinct
      t.id,
      t.fecha_creacion,
      r.suscriptor,
    from {{source("topups_web","ref_transaccion")}} t
      join {{source("topups_web","ref_recarga")}} r on (t.id=r.id_transaccion)
    where true
      and (t.id_estado!=20 AND r.id_estado!=27)
    order by t.fecha_creacion desc
  )
  select distinct
    e.id,
    suscriptor,
    t_actual as t_date,
    count(1) OVER (PARTITION BY suscriptor,e.id) AS cant_reintentos,
  from exitosas e 
    join fallidas f USING (suscriptor)
  where true 
    AND timestamp_diff(t_actual,fecha_creacion,HOUR)<=24
    AND (fecha_creacion BETWEEN t_anterior AND t_actual 
        OR (t_anterior IS NULL AND fecha_creacion < t_actual) )
  order by suscriptor, t_actual desc