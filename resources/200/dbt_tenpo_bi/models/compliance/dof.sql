{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}


with dates as (
    SELECT 
      fecha
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2014-10-29'), CURRENT_DATE(), INTERVAL 1 DAY)) fecha
  ),
  cruce as (
    SELECT DISTINCT
       dates.fecha,
       valor
     FROM dates
     LEFT JOIN  {{ source('ingestas_api', 'cmf_api_dolar')}} 
     dolar ON dates.fecha = dolar.fecha
  ),
  dolar_final as (
    SELECT DISTINCT
       fecha,
       IF(valor is null, LAST_VALUE(valor IGNORE NULLS) OVER (ORDER BY fecha ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), valor) valor_dolar_cierre
    FROM cruce
)

SELECT 
    u.uuid as user,
    m.fecha_creacion as trx_timestamp,
    DATE(m.fecha_creacion  , "America/Santiago") AS fecha,
    impfac  as monto,
    (impfac/valor_dolar_cierre) as monto_dolares,
    m.uuid as id_prp_movimiento,
    m.id_tx_externo as id_tx_api_externo,
    m.third_party_external_id as id_tx_tercero,
    c.tipo,
FROM {{ source('prepago', 'prp_cuenta') }} c
    JOIN {{ source('prepago', 'prp_usuario') }} u 
    ON c.id_usuario = u.id
    JOIN {{ source('prepago', 'prp_tarjeta') }} t
    ON c.id = t.id_cuenta
    JOIN {{ source('prepago', 'prp_movimiento') }} m 
    ON m.id_tarjeta = t.id
    LEFT JOIN dolar_final d
    ON DATE(m.fecha_creacion  , "America/Santiago") = DATE_SUB(d.fecha, INTERVAL 1 DAY)
WHERE 
    m.estado    = 'PROCESS_OK'
    AND indnorcor  = 0
    AND tipofac = 3001 -- Cashin TEF
    AND (impfac/valor_dolar_cierre) >= 1000
    AND origen_movimiento <> "SAT"
QUALIFY 
    row_number() over (partition by CAST(m.uuid AS string) order by m.fecha_actualizacion desc) = 1