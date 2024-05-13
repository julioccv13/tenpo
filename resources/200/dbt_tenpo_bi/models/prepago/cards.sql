{{ 
  config(
    materialized='ephemeral', 
  ) 
}}

WITH last_tarjetas AS (
    SELECT 
        -- Identificadores Users
        users.id  AS user
        -- UUID y ID: Cuenta y Tarjeta 
        ,c.uuid AS cuenta_uuid
        ,c.id AS cuenta_id
        ,t.uuid AS tarjeta_uuid
        ,t.id AS id_tarjeta

        -- Otros datos
        ,c.creacion AS fecha_creacion_cuenta
        ,tipo
        ,c.actualizacion AS fecha_actualizacion_cuenta
        ,t.estado AS estado_tarjeta_a_fecha_ejecucion_real_proceso -- ALERTA: este dato se va sobreescribiendo, potencialmente podria distorsionar la fecha de cierre.
        ,t.fecha_actualizacion AS fecha_actualizacion_tarjeta
        ,t.fecha_creacion AS fecha_creacion_tarjeta

        ,ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY t.fecha_actualizacion DESC) AS ro_num_actualizacion
    FROM `tenpo-airflow-prod.prepago.prp_cuenta` c
        JOIN `tenpo-airflow-prod.prepago.prp_usuario` u 
        ON c.id_usuario = u.id
        JOIN `tenpo-airflow-prod.users.users` users 
        ON u.uuid = users.id
        JOIN `tenpo-airflow-prod.prepago.prp_tarjeta` t 
        ON c.id = t.id_cuenta
        LEFT JOIN `tenpo-airflow-prod.prepago.prp_movimiento` m 
        ON m.id_tarjeta = t.id
    WHERE 
        TRUE 
        -- TODO: Conseguir historial de cambios de estados de tarjetas pasados para realizar correctamente el calculo.
        --AND c.estado = 'ACTIVE' 
        --AND u.estado = 'ACTIVE'

        -- Contaremos todas tarjetas, ya que en pasado pudieron haber estado activas.
)

SELECT 
    * EXCEPT(ro_num_actualizacion)
FROM last_tarjetas
WHERE ro_num_actualizacion = 1