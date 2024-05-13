DROP TABLE IF EXISTS `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj`;
CREATE TABLE `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj` AS (
  WITH 
    actividad as (   
      SELECT
        user
        ,LAST_VALUE(fecha) OVER (PARTITION BY user ORDER BY trx_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) fecha_ult_actividad
      FROM `${project_source_2}.economics.economics` 
      LEFT JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_0001_Params` b ON 1=1 
      WHERE 
        fecha <= Fecha_Fin_Analisis
        AND linea not in ('reward')
        ),
    users AS (
      SELECT 
          a.id user
          ,email
          ,Fecha_Fin_Analisis
          ,Fecha_Fin_Analisis_DT
          ,periodo
          ,DATE(a.ob_completed_at, "America/Santiago") fecha_ob
          ,DATETIME(ob_completed_at, "America/Santiago") dt_ob
          ,case when state in (7,8,21,22) then true else false end as cuenta_cerrada
          ,case when state in (21) then true else false end as cierre_involuntario
          ,case when state in (7,8,21,22) then DATETIME(updated_at, "America/Santiago") else null end as fecha_cierre
          ,CASE WHEN fecha_ult_actividad is not null then true else false end f_actividad_app
          ,ROW_NUMBER() over (PARTITION BY a.id ORDER BY updated_at desc) as row_num_actualizacion
      FROM `${project_source_3}.users.users` a
      LEFT JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_0001_Params` b ON 1=1 
      LEFT JOIN actividad d ON a.id = d.user
      WHERE 
          state in (4,7,8,21,22) 
          AND DATETIME(ob_completed_at, "America/Santiago") <= Fecha_Fin_Analisis
          AND ob_completed_at IS NOT NULL
    ),
    users_distinct as (
        SELECT 
            * EXCEPT(row_num_actualizacion)
        FROM users
        WHERE 
          row_num_actualizacion = 1
          )

  SELECT
    *
  FROM users_distinct
);


-- ONBOARDING EVENT
DROP TABLE IF EXISTS `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos`;
CREATE TABLE `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos` AS (
  SELECT 
  user
  ,Fecha_Fin_Analisis_DT
  ,periodo
  ,dt_ob as fecha_evento
  ,'[Backend] Onboarding completado' as event_name
  ,CAST(null as FLOAT64) as balance_amount
  ,f_actividad_app
  FROM `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj` 
);

-- CLEVERTAP EVENTS
INSERT `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos`
SELECT 
    user
    ,Fecha_Fin_Analisis_DT
    ,periodo
    ,datetime(TIMESTAMP_ADD(e.fecha_hora, interval 4 hour)  , "America/Santiago") fecha_hora --corrección por falla formato en clevertap events
    ,'[CleverTap] ' || event
    ,CAST(null as FLOAT64) as balance_amount
    ,f_actividad_app
FROM `${project_source_1}.clevertap.events` e
JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj` u on e.email = u.email AND  datetime(TIMESTAMP_ADD(e.fecha_hora, interval 4 hour)  , "America/Santiago") <= Fecha_Fin_Analisis
WHERE
  event in ('App Installed', 'App Uninstalled','App Launched', 'Hace login', 'Session Concluded')
  AND (e.identity is not null or e.email is not null)
  AND DATETIME(e.fecha_hora, "America/Santiago") >= fecha_ob
  AND 
    (
      (event = 'Session Concluded' AND session_lenght > 0)
      OR
      (event <> 'Session Concluded')
    );

-- ECONOMICS EVENTS
INSERT `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos`
SELECT DISTINCT 
  e.user
  ,Fecha_Fin_Analisis_DT
  ,periodo
  ,DATETIME(trx_timestamp, "America/Santiago") trx_timestamp
  ,'[Economics] ' || linea as event_name
  ,CAST(null as FLOAT64) as balance_amount
  ,f_actividad_app
FROM `${project_source_2}.economics.economics` e 
JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj`  u on u.user = e.user AND DATETIME(trx_timestamp, "America/Santiago") <= Fecha_Fin_Analisis
WHERE 
  e.fecha >= fecha_ob;

-- CIERRES DE CUENTA BACKEND
INSERT `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos`
SELECT DISTINCT 
  user
  ,Fecha_Fin_Analisis_DT
  ,periodo
  ,fecha_cierre
  ,'[Backend] Cierre de Cuenta' as event_name
  ,CAST(null as FLOAT64) as balance_amount
  ,f_actividad_app
FROM `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj` 
WHERE 
  datetime(fecha_cierre) <= Fecha_Fin_Analisis
  and cuenta_cerrada is true;


INSERT `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos`
SELECT 
    distinct 
    a.user
    ,Fecha_Fin_Analisis_DT
    ,periodo
    ,datetime(fecha) --corrección por falla formato en clevertap events
    ,'[Saldo] Registro'
    ,CAST(saldo_dia as FLOAT64) as balance_amount
    ,f_actividad_app
FROM `${project_source_2}.balance.daily_balance` a
JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj` b 
ON a.user = b.user and a.fecha <= b.Fecha_Fin_Analisis;

DROP TABLE IF EXISTS `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`;
CREATE TABLE `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo` AS (
WITH 
  data as (
    SELECT
      *
      ,CASE WHEN periodo = "monthly" THEN extract(year FROM fecha_evento)*12 + extract(month FROM fecha_evento) 
       WHEN periodo = "weekly" THEN IF( extract(week FROM fecha_evento)<10 , cast(concat(cast(extract(year FROM fecha_evento) as STRING),"0", cast(extract(week FROM fecha_evento) as STRING)) as INT64) , cast(concat(cast(extract(year FROM fecha_evento) as STRING),CAST(extract(week FROM fecha_evento) as STRING)) as INT64))
       END as periodo_orden
      ,CASE WHEN periodo = "monthly" THEN date_trunc(fecha_evento, month) 
       WHEN periodo = "weekly" THEN date_trunc(fecha_evento, week)
       END as periodo_legible
      ,CASE WHEN event_name in ( '[Backend] Cierre de Cuenta' ) OR (event_name = '[CleverTap] App Uninstalled') THEN 1 ELSE 0 
       END AS ultimo_evento_periodo_es_churn 
      --,if(event_name in ('[CleverTap] App Uninstalled', '[Backend] Cierre de Cuenta'), 1, 0) as ultimo_evento_periodo_es_churn
    FROM `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos`
    ), 
  agg_data_periodo as (
    SELECT 
      user
      ,Fecha_Fin_Analisis_DT
      ,periodo_orden
      ,coalesce(max(if(event_name = '[Backend] Onboarding completado', 1, 0)), 0) as onboarding_periodo
      ,coalesce(max(if(event_name in ( '[Backend] Cierre de Cuenta' ), 1, 0)), 0) as cierre_cuenta_periodo
    FROM data
    GROUP BY 
    1,2,3
      ), 
  balance_data as (
    SELECT
      * except(balance_amount)
      ,coalesce(balance_amount, last_value(balance_amount ignore nulls) over (PARTITION BY user ORDER BY fecha_evento rows between unbounded preceding and current row)) as balance_amount
    FROM data
    ORDER BY 
      fecha_evento desc
  ),
  ultimo_evento as (
    SELECT
      *
      ,ROW_NUMBER() OVER (PARTITION BY user, periodo_orden ORDER BY fecha_evento desc) as posicion_periodo
    FROM balance_data
    WHERE event_name <> '[Saldo] Registro' 
    ORDER BY 
      fecha_evento desc
  )

  SELECT
    a.*
    ,b.* except(user, Fecha_Fin_Analisis_DT, periodo_orden)
  FROM ultimo_evento a 
  LEFT JOIN agg_data_periodo b on a.user = b.user and a.Fecha_Fin_Analisis_DT = b.Fecha_Fin_Analisis_DT and a.periodo_orden = b.periodo_orden
  WHERE
    posicion_periodo = 1 -- último evento del periodo
);

INSERT INTO `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`
SELECT
  a.*
FROM (
    SELECT 
      user
      ,Fecha_Fin_Analisis_DT
      ,CAST(null as STRING) as periodo
      ,Fecha_Fin_Analisis as fecha_evento
      ,'Dummy event (user sin eventos este periodo)' event_name
      ,CAST(null as BOOL) f_actividad_app
      ,periodo_orden
      ,periodo_legible
      ,null as ultimo_evento_periodo_es_churn
      ,CAST(null as FLOAT64) as balance_amount
      ,1 as posicion_periodo
      ,null as onboarding_periodo
      ,null as cierre_cuenta_periodo
    FROM `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj`
    LEFT JOIN (SELECT DISTINCT periodo_orden, periodo_legible FROM `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`) ON 1=1
    WHERE 
       case when periodo = "monthly" then date_trunc(fecha_ob, month) 
       when periodo = "weekly" then date_trunc(fecha_ob, week) end <= date(periodo_legible)
      ) a 
LEFT JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo` b ON a.user = b.user and a.periodo_orden = b.periodo_orden
WHERE 
  b.user is null; 

DROP TABLE IF EXISTS `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux`;
CREATE TABLE `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux` AS (
  WITH 
    coalesced_data as (
      SELECT 
        *
        ,coalesce(balance_amount, last_value(balance_amount ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row), 0) as coalesced_balance_amount
        ,coalesce(f_actividad_app, last_value(f_actividad_app ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row)) as coalesced_f_actividad_app
        ,coalesce(periodo, last_value(periodo ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row)) as coalesced_periodo
        ,coalesce(ultimo_evento_periodo_es_churn, last_value(ultimo_evento_periodo_es_churn ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row)) as coalesced_churn
        ,max(cierre_cuenta_periodo) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row) as cierre_de_cuenta_pasado
      FROM `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`
        ), 
    lagged_data as (
      SELECT 
        *
        ,lag(coalesced_churn) over (PARTITION BY user ORDER BY periodo_orden) as churn_periodo_pasado
      FROM coalesced_data
      ), 
    retornos as (
      SELECT 
        *
        ,case when churn_periodo_pasado = 1 and coalesced_churn = 0 then 1 else 0 end as retorno_periodo
      FROM lagged_data
        ),
    statet as (
      SELECT
        *
        ,case when cierre_de_cuenta_pasado = 1 then 'cuenta_cerrada'
          when coalesce(ultimo_evento_periodo_es_churn, churn_periodo_pasado, 0) = 1 and coalesce(coalesced_balance_amount, 0) < 1000 then 'churneado'
          else 'activo' end as state
      FROM retornos
      ),
    last_statet as (
      SELECT
        *
        ,coalesce(lag(state) over (partition by user order by periodo_orden), 'onboarding') as last_state
      FROM statet
      )

  SELECT
    a.*
    ,b.cierre_involuntario
  FROM last_statet a
  LEFT JOIN `${project_target}.temp.CHURN_{{period}}_{{ds_nodash}}_102_PubObj`  b ON a.user = b.user
);  


