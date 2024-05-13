{% set partitions_between_to_replace = [
    'date_sub(current_date, interval 5 day)',
    'current_date'
] %}

{{ 
  config(
    materialized='table', 
    tags=["daily", "bi"],
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite'
  )
}}

SELECT
      DISTINCT
      DATE(t.fecha_creacion) fecha,
      t.fecha_creacion fecha_creacion_tarjeta,
      c.id as numero_contrato,
      t.tipo,
      t.red,
      u.uuid as user,
      u.estado estado_user,
FROM {{ source('prepago', 'prp_cuenta') }} c
JOIN {{ source('prepago', 'prp_tarjeta') }} t ON c.id = t.id_cuenta
JOIN {{ source('prepago', 'prp_usuario') }} u ON c.id_usuario = u.id
WHERE red <> 'WHITE_LABEL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY fecha_creacion_tarjeta ASC) = 1

{% if is_incremental() %}
    and fecha between {{ partitions_between_to_replace | join(' and ') }}
{% endif %}