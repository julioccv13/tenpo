{% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval 3 day)'
] %}


{{ 
  config(
    materialized='incremental'
    ,tags=["daily", "bi", "datamart"]
    ,project=env_var('DBT_PROJECT11', 'tenpo-datalake-sandbox')
    ,partition_by = { 'field': 'fecha_creacion_usuario', 'data_type': 'date' }
    ,incremental_strategy = 'insert_overwrite'
  )
}}

select distinct
  a.* ,
  u.* except(id,fecha_creacion,fecha_actualizacion,uuid),
  cast(u.fecha_creacion as date) as fecha_creacion_usuario,
  u.fecha_creacion as ts_creacion_usuario,
  u.fecha_actualizacion as fecha_actualizacion_usuario,
  c.* except(id,uuid,estado,nivel), 
  c.uuid as uuid_capa_c,
  c.estado as estado_cuenta,
  t.* except(id,uuid,estado,fecha_creacion,fecha_actualizacion,tipo), 
  t.id as id_tarjeta,
  t.estado as  estado_tarjeta,
  t.fecha_creacion as fecha_creacion_tarjeta,
  t.fecha_actualizacion as fecha_actualizacion_tarjeta
from {{source('accounts','accounts')}} a
  join {{ source('prepago', 'prp_usuario') }} u on (a.user_id=u.uuid)
  join {{ source('prepago', 'prp_cuenta') }} c on (u.id=c.id_usuario)
  join {{ source('prepago', 'prp_tarjeta') }} t on (t.id_cuenta=c.id)
where true
  {% if is_incremental() %}
    and cast(u.fecha_creacion as date) in ({{ partitions_to_replace | join(',') }})
  {% endif %}
