{{ config(materialized='table') }}

with data as (
    SELECT distinct
        CASE 
        WHEN t.id_usuario  > 0 THEN t.correo_usuario
        WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN t.correo_usuario
        ELSE NULL
        END
        AS email_tu,
        LAST_VALUE(concat("56",r.suscriptor)) OVER (PARTITION BY t.correo_usuario order by t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as phone_tu,
        LAST_VALUE(tp.nombre) OVER (PARTITION BY t.correo_usuario order by t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as categoria,
        LAST_VALUE(o.nombre) OVER (PARTITION BY t.correo_usuario order by t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as operador,
        FIRST_VALUE(t.fecha_creacion) OVER (partition by t.correo_usuario order by t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)  as ts_creacion_topup,
        LAST_VALUE(t.fecha_creacion) OVER (partition by t.correo_usuario order by t.fecha_creacion RANGE BETWEEN UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) as last_topup,
        FROM {{ source('topups_web', 'ref_transaccion') }} t 
        LEFT JOIN {{ source('topups_web', 'bof_persona') }} bp ON t.id_usuario = bp.id_persona 
        INNER JOIN {{ source('topups_web', 'ref_recarga') }} r ON t.id = r.id_transaccion
        INNER JOIN {{ source('topups_web', 'ref_producto') }} p ON p.id = r.id_producto 
        INNER JOIN {{ source('topups_web', 'ref_operador') }} o ON o.id = p.id_operador
        INNER JOIN {{ source('topups_web', 'ref_comisiones') }} c ON c.id_producto = p.id 
        INNER JOIN {{ source('topups_web', 'ref_tipo_producto') }} tp ON tp.id = p.id_tipo_producto 
        WHERE t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5) AND t.correo_usuario <> "" 
)

select
    {{ hash_sensible_data('email_tu') }} as email_tu
    ,phone_tu
    ,categoria
    ,operador
    ,ts_creacion_topup
    ,last_topup
from data