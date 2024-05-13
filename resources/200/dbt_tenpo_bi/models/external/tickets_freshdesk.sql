{{config(
    materialized='table',
    tags=["daily"],
  ) 
}}

WITH 
   rfmp as (
    SELECT DISTINCT
      user id,
      email,
      LAST_VALUE(Fecha_Fin_Analisis_DT) OVER (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ultima_fecha_rfmp_60,
      LAST_VALUE(segment_ult60d) OVER (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) ultimo_segmento_rfmp_60
    FROM {{source('tablones_analisis','tablon_rfmp_v2')}}
  ),
   datos_dataflow as (        
    SELECT DISTINCT
      ticket_id id,
      user_id user,
      ticket_subject  asunto, 
      ticket_status estado, 
      ticket_priority prioridad, 
      ticket_source origen, 
      if( ticket_ticket_type is null, "Sin tipo", ticket_ticket_type) tipo, 
      ticket_agent_name agente , 
      ticket_group_name grupo, 
      TIMESTAMP_SUB(ticket_created_at, INTERVAL 3 HOUR) creacion ,
      TIMESTAMP_SUB(ticket_first_responded_at, INTERVAL 3 HOUR) respuesta_inicial,
      TIMESTAMP_SUB(ticket_updated_at, INTERVAL 3 HOUR) actualizacion,
      TIMESTAMP_SUB(ticket_resolved_at, INTERVAL 3 HOUR)   resolucion,
      TIMESTAMP_SUB(ticket_closed_at, INTERVAL 3 HOUR)  cierre,
      ticket_product_description producto, 
      agent_iterations interacciones_agente,
      IF(ticket_subject ='CARD_ACTIVATION', 'Card Activation',IF(ticket_subject ='ACCOUNT_CONFIG', 'Cierre de cuenta', 
       IF( ticket_cf_subtipificacin_del_caso is null, 'Sin tipificar', ticket_cf_subtipificacin_del_caso ))) subtipificacion
    FROM {{source('freshdesk','freshdesk')}}
        ),
        
    historia as (   
      SELECT DISTINCT 
        id, 
        user,
        asunto, 
        estado, 
        prioridad, 
        origen, 
        if(tipo is null OR length(tipo) < 1, "Sin tipo", tipo) tipo, 
        agente, 
        grupo, 
        creacion ,
        respuesta_inicial,
        actualizacion,
        resolucion,
        cierre,
        producto, 
        interacciones_agente,
        IF(asunto ='CARD_ACTIVATION', 'Card Activation',IF(asunto ='ACCOUNT_CONFIG', 'Cierre de cuenta', 
         IF( subtipificacion is null, 'Sin tipificar', subtipificacion ))) subtipificacion
      FROM {{source('freshdesk_external','historia')}}
      ),

  historia_2022_01_01_2022_07_08 as (
    SELECT CAST(ID_del_ticket AS INT64) as id,
      User_Id as user,
      Asunto as asunto,
      Estado as estado,
      Prioridad as prioridad,
      Origen as origen,
      if( Tipo is null, "Sin tipo", Tipo) as tipo,
      Agente as agente,
      Grupo as grupo,
      creacion_utc as creacion,
      TIMESTAMP_SUB(Tiempo_de_respuesta_inicial_utc, INTERVAL 3 HOUR) as respuesta_inicial,
      TIMESTAMP_SUB(Hora_de_ultima_actualizacion_utc, INTERVAL 3 HOUR) as actualizacion,
      TIMESTAMP_SUB(Hora_de_resolucion_utc, INTERVAL 3 HOUR) as resolucion,
      TIMESTAMP_SUB(Hora_de_cierre_utc, INTERVAL 3 HOUR) as cierre,
      Producto as producto,
      CAST(Interacciones_del_agente AS INT64) AS interacciones_agente,
      IF(asunto ='CARD_ACTIVATION', 'Card Activation',IF(asunto ='ACCOUNT_CONFIG', 'Cierre de cuenta', 
        IF(Subtipificacion_del_caso is null, 'Sin tipificar', Subtipificacion_del_caso ))) as subtipificacion
      FROM `tenpo-external.freshdesk.tickets_2022_01_01-2022_07_08_zona_horaria` 

  ),


    historia_2022_07_08_2022_09_30 as (
    SELECT CAST(ID_del_ticket AS INT64) as id,
      User_Id as user,
      Asunto as asunto,
      Estado as estado,
      Prioridad as prioridad,
      Origen as origen,
      if( Tipo is null, "Sin tipo", Tipo) as tipo,
      Agente as agente,
      Grupo as grupo,
      creacion_utc as creacion,
      TIMESTAMP_SUB(Tiempo_de_respuesta_inicial_utc, INTERVAL 3 HOUR) as respuesta_inicial,
      TIMESTAMP_SUB(Hora_de_ultima_actualizacion_utc, INTERVAL 3 HOUR) as actualizacion,
      TIMESTAMP_SUB(Hora_de_resolucion_utc, INTERVAL 3 HOUR) as resolucion,
      TIMESTAMP_SUB(Hora_de_cierre_utc, INTERVAL 3 HOUR) as cierre,
      Producto as producto,
      CAST(Interacciones_del_agente AS INT64) AS interacciones_agente,
      IF(asunto ='CARD_ACTIVATION', 'Card Activation',IF(asunto ='ACCOUNT_CONFIG', 'Cierre de cuenta', 
        IF(Subtipificacion_del_caso is null, 'Sin tipificar', Subtipificacion_del_caso ))) as subtipificacion
      FROM `tenpo-external.freshdesk.tickets_2022_07_08-2022_09_30_zona_horaria`

  ),

    tickets_daily as ( SELECT
    CAST(Ticket_ID AS INT64) as id,
    CAST(null as string) as user,
    Subject as asunto,
    Status as estado,
    Priority as prioridad,
    Source as origen,
    if(Ticket_type is null, "Sin tipo", Ticket_type) as tipo,
    First_assigned_agent as agente,
    First_assigned_group as grupo,
    PARSE_TIMESTAMP("%Y/%m/%d %I:%M:%S %p", replace(Created_date,'-','/')) as creacion,
    TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y/%m/%d %I:%M:%S %p", replace(First_response_date,'-','/')), INTERVAL 0 HOUR) as respuesta_inicial,
    TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y/%m/%d %I:%M:%S %p", replace(Last_updated_date,'-','/')), INTERVAL 0 HOUR) as actualizacion,
    TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y/%m/%d %I:%M:%S %p", replace(Resolved_date,'-','/')), INTERVAL 0 HOUR) as resolucion,
    TIMESTAMP_SUB(PARSE_TIMESTAMP("%Y/%m/%d %I:%M:%S %p", replace(Closed_date,'-','/')), INTERVAL 0 HOUR) as cierre,
    Product as producto,
    null AS interacciones_agente,
    IF(Subject ='CARD_ACTIVATION', 'Card Activation',IF(Subject ='ACCOUNT_CONFIG', 'Cierre de cuenta', 
      IF(Subtipificacion_del_caso is null, 'Sin tipificar', Subtipificacion_del_caso ))) as subtipificacion
    FROM `tenpo-bi-prod.external.tickets_freshdesk_daily`
    ), 
        
  datos as (
      SELECT 
        *
      FROM datos_dataflow
      
      UNION ALL
      
      SELECT 
        *
      FROM historia
      
      UNION ALL

      SELECT
        *
      FROM
      historia_2022_01_01_2022_07_08

      UNION ALL

      SELECT
        *
      FROM
      historia_2022_07_08_2022_09_30

      UNION ALL

      SELECT
        *
      FROM 
      tickets_daily
      ),
   
  target as (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY actualizacion DESC) as ro_num_actualizacion
    FROM datos
    ),
  
  target_last_value as (
    SELECT DISTINCT
      id
      ,LAST_VALUE(user IGNORE NULLS) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)  user
      ,LAST_VALUE(asunto) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) asunto
      ,LAST_VALUE(estado) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) estado
      ,LAST_VALUE(prioridad) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) prioridad
      ,LAST_VALUE(origen) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) origen
      ,LAST_VALUE(tipo) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) tipo
      ,LAST_VALUE(agente) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) agente
      ,LAST_VALUE(grupo) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) grupo
      ,LAST_VALUE(creacion IGNORE NULLS) OVER (PARTITION BY id ORDER BY creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) creacion
      ,LAST_VALUE(respuesta_inicial IGNORE NULLS) OVER (PARTITION BY id ORDER BY respuesta_inicial DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) respuesta_inicial
      ,LAST_VALUE(actualizacion IGNORE NULLS) OVER (PARTITION BY id ORDER BY actualizacion ASC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) actualizacion
      ,LAST_VALUE(resolucion IGNORE NULLS) OVER (PARTITION BY id ORDER BY resolucion DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) resolucion
      ,LAST_VALUE(cierre IGNORE NULLS) OVER (PARTITION BY id ORDER BY cierre DESC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) cierre
      ,LAST_VALUE(producto) OVER (PARTITION BY id ORDER BY actualizacion ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) producto
      ,LAST_VALUE(interacciones_agente) OVER (PARTITION BY id ORDER BY actualizacion ASC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) interacciones_agente
      ,LAST_VALUE(subtipificacion) OVER (PARTITION BY id ORDER BY actualizacion ASC  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) subtipificacion
    FROM target
  
  ),
  
  calculos as (
    SELECT DISTINCT
      *,
      IF(cierre = creacion, 0, TIMESTAMP_DIFF(respuesta_inicial,creacion, MINUTE)) primer_tiempo_respuesta, 
      IF(interacciones_agente = 1 and cierre is not null , 'cumple', 'no cumple') cierre_en_primera_respuesta,
      TIMESTAMP_DIFF(actualizacion ,creacion, MINUTE) act_creacion,
      TIMESTAMP_DIFF(if(cierre is null, (SELECT MAX(cierre) FROM target )  , cierre) ,creacion, MINUTE) cierre_creacion,
      TIMESTAMP_DIFF(cierre, creacion, MINUTE) cierre_creacion_ptr,
      IF(estado = "Resolved", resolucion , if(cierre is null,(SELECT MAX(cierre) FROM target ) , cierre)) timestamp_mttr,
      TIMESTAMP_DIFF(if(estado = "Resolved", resolucion , if(cierre is null,(SELECT MAX(cierre) FROM  target ) , cierre)) , creacion, MINUTE)/24/60 mttr,
    FROM target_last_value -- FROM target
--     WHERE 
--       ro_num_actualizacion = 1
  ), categorias as (

    SELECT DISTINCT
      c.*,
      ultimo_segmento_rfmp_60,
      CASE WHEN producto in ( 'Recarga Fácil' , 'Recargatucelular.cl', 'Recarga.cl') THEN 'WEB-Recargas'
      WHEN producto in ('Tarjeta Prepago Mastercard', 'Bolsillo', 'Bolsillo in App') THEN 'APP-Tenpo'
      WHEN producto = 'TenpoPayPal' THEN 'WEB-PayPal'
      ELSE 'OTRO' end as producto_recod,
      IF(primer_tiempo_respuesta < 90, 'cumple', 'no cumple') prim_tiempo_menor_90,
      IF(primer_tiempo_respuesta < 90 and cierre_creacion_ptr < 90, 'cumple', 'no cumple') cierre_menor_90,
  FROM calculos c
  LEFT JOIN rfmp on rfmp.id = c.user
  WHERE tipo not in ('Ticket Prueba', 'Regularizacion Alta Rapida', 'Cierre de cuenta - Entidad')
  
  )

SELECT
    categorias.*,
    IFNULL(d.categoria,'consulta') categoria
FROM categorias
LEFT JOIN {{source('diccionario_tickets_freshdesk','diccionario_tickets_freshdesk')}} d USING (tipo,producto_recod)




