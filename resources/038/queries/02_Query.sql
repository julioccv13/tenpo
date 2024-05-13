DROP TABLE IF EXISTS `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_102_Temp`;
CREATE TABLE `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_102_Temp` AS (

 WITH 
  datos_tarjetas as (
    SELECT 
        Fecha_Fin_Analisis_DT
        ,c.uuid as cuenta_uuid
        ,c.id as cuenta_id
        ,t.uuid as tarjeta_uuid
        ,t.id as id_tarjeta
        ,c.creacion as fecha_creacion_cuenta
        ,c.actualizacion as fecha_actualizacion_cuenta
        ,t.estado as estado_tarjeta_a_fecha_ejecucion_real_proceso -- este dato se va sobreescribiendo.
        ,t.fecha_actualizacion as fecha_actualizacion_tarjeta
        ,t.fecha_creacion as fecha_creacion_tarjeta
        ,u.uuid user
        ,t.tipo
        ,red
    FROM `${project_source}.prepago.prp_cuenta`  c
    JOIN `${project_source}.prepago.prp_usuario`  u ON c.id_usuario = u.id
    JOIN `${project_source}.prepago.prp_tarjeta`  t ON c.id = t.id_cuenta
    LEFT JOIN `${project_source}.prepago.prp_movimiento`  m ON m.id_tarjeta = t.id
    LEFT JOIN `${project_target}.temp.Calculo_Tarjetas_{{period}}_{{ds_nodash}}_003_Params` ON 1= 1
     WHERE 
      TRUE 
      AND c.estado = 'ACTIVE' 
      AND u.estado = 'ACTIVE'
      AND t.estado IN  ('ACTIVE', 'LOCKED')      
      AND ((tipofac in (336) AND m.estado  in ('PROCESS_OK') AND m.estado_de_negocio  in ('CONFIRMED','OK') AND id_tx_externo not like 'MC_%' AND indnorcor  = 0)
       OR (m.estado = 'PROCESS_OK' AND indnorcor  = 0 AND tipofac in (3001,3002) ) 
       OR (m.estado = 'PROCESS_OK' AND indnorcor  = 0 AND tipofac in (3032))
       OR t.tipo = "PHYSICAL")
      AND  DATE(t.fecha_actualizacion) <= Fecha_Fin_Analisis_DT
       ),
  last_tarjetas as (
    SELECT 
        * 
        ,ROW_NUMBER() OVER (PARTITION BY id_tarjeta, tipo ORDER BY fecha_actualizacion_tarjeta DESC) as ro_num_actualizacion
    FROM datos_tarjetas
    )
           
    SELECT DISTINCT
      Fecha_Fin_Analisis_DT
      ,user
      ,tipo
      ,red
      ,fecha_creacion_cuenta fecha_activacion
      ,'Activa tarjeta' as paso
      ,fecha_actualizacion_tarjeta
      ,COUNT(DISTINCT tarjeta_uuid) cuenta_tarjetas
    FROM last_tarjetas
    WHERE TRUE
      AND estado_tarjeta_a_fecha_ejecucion_real_proceso = 'ACTIVE'
      AND ro_num_actualizacion = 1
    GROUP BY 
      1,2,3,4,5,6,7

);