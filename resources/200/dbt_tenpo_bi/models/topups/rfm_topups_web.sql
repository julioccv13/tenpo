{{ config(materialized='table',  tags=["daily", "bi"]) }}


with 
  vars as (
    SELECT 
      60 as time_to_churn,
  ),
  datos as (
    with datos_topups as (
      SELECT DISTINCT
        FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_order_date,
        FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_order_date,
        AVG(t.monto) OVER (PARTITION BY tp.nombre,t.correo_usuario ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as avg_amount,
        COUNT(*) OVER (PARTITION BY tp.nombre,t.correo_usuario ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as count_trx,
        "Exitosa" as estado,
        FIRST_VALUE(r.suscriptor) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as cli,
        FIRST_VALUE(
          CASE 
            WHEN t.id_usuario  > 0 THEN 'Registrado'
            WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN 'Semiregistrado'
            ELSE 'Anónimo' 
            END
          ) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as tipo_usuario,
        {{target.schema}}.accent2latin(correo_usuario) AS correo_contacto,
        REGEXP_EXTRACT({{target.schema}}.accent2latin(correo_usuario), r'@(.+)') as dominio,
        tp.nombre as categoria,
        FIRST_VALUE(o.nombre) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as operador,
        IF(users.email IS NULL, "No", "Si") as entenpo,
        date_diff(
          FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
          NTH_VALUE(DATE(t.fecha_creacion),2) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
          DAY) as before_last_order_days,
        REGEXP_CONTAINS({{target.schema}}.accent2latin(correo_usuario), r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") AS is_valid,
        CASE 
          WHEN users.email IS NULL THEN "WEB"
          WHEN FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= last_tu.last_tu_app THEN  "APP"
          WHEN FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) > last_tu.last_tu_app THEN  "WEB"
          ELSE "WEB"
          END  as last_tu_source
      FROM {{source("topups_web","ref_transaccion")}}  t
        INNER JOIN {{source("topups_web","ref_recarga")}} r ON t.id = r.id_transaccion
        INNER JOIN {{source('topups_web','ref_producto')}} p ON p.id = r.id_producto 
        INNER JOIN {{source('topups_web','ref_operador')}} o ON o.id = p.id_operador
        INNER JOIN {{source('topups_web','ref_comisiones')}} c ON c.id_producto = p.id 
        INNER JOIN {{source('topups_web','ref_tipo_producto')}} tp ON tp.id = p.id_tipo_producto
        LEFT JOIN (
          SELECT DISTINCT 
            email
          FROM {{source('tenpo_users','users')}}) users on users.email = t.correo_usuario 
        LEFT JOIN (
          SELECT DISTINCT 
            u.email,
            LAST_VALUE(e.fecha) OVER (PARTITION BY u.email ORDER BY e.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_tu_app 
          FROM {{source('tenpo_users','users')}}u 
            JOIN {{ref("economics")}} e ON (u.id=e.user) where linea="top_ups" ) last_tu on last_tu.email = t.correo_usuario 
      WHERE t.id_origen IN (1,2,5) AND t.id_estado = 20 AND r.id_estado = 27
    ),
    datos_topups_fallidos as (
      SELECT DISTINCT
        FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_order_date,
        FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_order_date,
        AVG(t.monto) OVER (PARTITION BY tp.nombre,t.correo_usuario ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as avg_amount,
        COUNT(*) OVER (PARTITION BY tp.nombre,t.correo_usuario ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as count_trx,
        "Fallida" as estado,
        FIRST_VALUE(r.suscriptor) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as cli,
        FIRST_VALUE(
        CASE 
          WHEN t.id_usuario  > 0 THEN 'Registrado'
          WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN 'Semiregistrado'
          ELSE 'Anónimo' 
          END
        ) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC) as tipo_usuario,
        {{target.schema}}.accent2latin(correo_usuario) AS correo_contacto,
        REGEXP_EXTRACT({{target.schema}}.accent2latin(correo_usuario), r'@(.+)') as dominio,
        tp.nombre as categoria,
        FIRST_VALUE(o.nombre) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as operador,
        IF(users.email IS NULL, "No", "Si") as entenpo,
        date_diff(
          FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
          NTH_VALUE(DATE(t.fecha_creacion),2) OVER (PARTITION BY t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
          DAY) as before_last_order_days,
        REGEXP_CONTAINS({{target.schema}}.accent2latin(correo_usuario), r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+") AS is_valid,
        CASE 
          WHEN users.email IS NULL THEN "WEB"
          WHEN FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) <= last_tu.last_tu_app THEN  "APP"
          WHEN FIRST_VALUE(DATE(t.fecha_creacion)) OVER (PARTITION BY tp.nombre,t.correo_usuario ORDER BY t.fecha_creacion DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) > last_tu.last_tu_app THEN  "WEB"
          ELSE "WEB"
          END  as last_tu_source
      FROM {{source("topups_web","ref_transaccion")}}  t
        INNER JOIN {{source("topups_web","ref_recarga")}} r ON t.id = r.id_transaccion
        INNER JOIN {{source('topups_web','ref_producto')}} p ON p.id = r.id_producto 
        INNER JOIN {{source('topups_web','ref_operador')}} o ON o.id = p.id_operador
        INNER JOIN {{source('topups_web','ref_comisiones')}} c ON c.id_producto = p.id 
        INNER JOIN {{source('topups_web','ref_tipo_producto')}} tp ON tp.id = p.id_tipo_producto
        LEFT JOIN (
          SELECT DISTINCT 
            email 
          FROM {{source('tenpo_users','users')}}
          ) users on users.email = t.correo_usuario
        LEFT JOIN (
          SELECT DISTINCT 
            u.email,
            LAST_VALUE(e.fecha) OVER (PARTITION BY u.email ORDER BY e.fecha RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_tu_app 
          FROM {{source('tenpo_users','users')}}u 
            JOIN {{ref("economics")}} e ON (u.id=e.user) 
          where linea="top_ups" ) last_tu on last_tu.email = t.correo_usuario 
      WHERE t.id_origen IN (1,2,5) AND (t.id_estado <> 20 OR r.id_estado <> 27)
    )
    select distinct *,
     DATE_DIFF(CURRENT_DATE(),last_order_date,DAY) as recency,
     DATE_DIFF(CURRENT_DATE(),first_order_date,DAY) as days_first_trx,
     ntile(4) over (partition by estado,categoria order by last_order_date) as rfm_recency,
     ntile(4) over (partition by estado,categoria order by count_trx) as rfm_frequency,
     ntile(4) over (partition by estado,categoria order by avg_amount) as rfm_monetary
    from 
      datos_topups
    UNION ALL
    select distinct *,
     DATE_DIFF(CURRENT_DATE(),last_order_date,DAY) as recency,
     DATE_DIFF(CURRENT_DATE(),first_order_date,DAY) as days_first_trx,
     ntile(4) over (partition by estado,categoria order by last_order_date) as rfm_recency,
     ntile(4) over (partition by estado,categoria order by count_trx) as rfm_frequency,
     ntile(4) over (partition by estado,categoria order by avg_amount) as rfm_monetary
    from 
      datos_topups_fallidos
    WHERE correo_contacto NOT IN (select distinct correo_contacto from datos_topups)
      AND cli NOT IN (select distinct cli from datos_topups)
    ),
  desuscritos as (
    select 
          email as correo_contacto,
        date as fecha_desuscrito,
    from {{source('aux_table','unsubscribe_web')}} 
    WHERE source = "Topups"
  ),
  puntos as (
  select 
    saldo as saldo_puntos,
    correo as correo_contacto
  from {{ref('puntos_rf')}}
  )
  select distinct 
    {{ hash_sensible_data('correo_contacto') }} as correo_contacto,
    * except(correo_contacto),
    CASE 
      WHEN recency > (SELECT time_to_churn FROM vars)  THEN "Abandono"
      WHEN before_last_order_days IS NULL THEN "Nuevo"
      WHEN before_last_order_days > (SELECT time_to_churn FROM vars) THEN "Retorno"
      WHEN before_last_order_days <= (SELECT time_to_churn FROM vars) THEN "Recurrente"
      ELSE "Revisar"
      END as tipo,
    rfm_recency*100 + rfm_frequency*10 + rfm_monetary as rfm_combined,
    rfm_recency + rfm_frequency + rfm_monetary as rfm_sum
  from datos
    left join (
      select 
        correo as correo_contacto,
        campana as nombre_campana,
        fecha_ini as fecha_campana, 
        date_diff(current_date(), fecha_ini, DAY) as campaign_days 
      from {{source('aux_topups','aux_campanas')}}
    ) as campanas USING (correo_contacto)
    left join (
      select 
        correo as correo_contacto,
        last_value(campana) OVER(PARTITION BY correo ORDER BY fecha_ini ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_campaign,
        last_value(fecha_ini) OVER(PARTITION BY correo ORDER BY fecha_ini ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as date_last_campaign,
        date_diff(current_date(), last_value(fecha_ini) OVER(PARTITION BY correo ORDER BY fecha_ini ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), DAY) as days_last_campaign 
      from {{source('aux_topups','aux_campanas')}}
    ) as last_campaign USING (correo_contacto)
    LEFT JOIN desuscritos USING (correo_contacto)
    LEFT JOIN puntos USING (correo_contacto)