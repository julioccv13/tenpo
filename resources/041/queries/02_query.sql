DROP TABLE IF EXISTS `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj`;
CREATE TABLE `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj` AS (
  WITH 
    actividad as (   -- Se definde la actividad en bolsillo como un usuario que tenga al menos un aporte
      SELECT
        user
        ,LAST_VALUE(fecha) OVER (PARTITION BY user ORDER BY trx_timestamp ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) fecha_ult_actividad
      FROM `${project_source_1}.economics.economics` 
      LEFT JOIN `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_0001_Params` b ON 1=1 
      WHERE 
        fecha <= Fecha_Fin_Analisis
        AND linea in ('cash_in_savings')
        ),
    users AS (
      SELECT 
          a.id user
          ,a.email 
          ,Fecha_Fin_Analisis
          ,Fecha_Fin_Analisis_DT
          ,periodo
          ,DATE(a.onboarding_date , "America/Santiago") fecha_ob
          ,DATETIME(a.onboarding_date , "America/Santiago") dt_ob
--           ,case when state in (7,8) then true else false end as cuenta_cerrada
--           ,case when state in (7,8) then DATETIME(updated_at, "America/Santiago") else null end as fecha_cierre
          ,CASE WHEN fecha_ult_actividad is not null then true else false end f_actividad_app
          ,ROW_NUMBER() over (PARTITION BY a.id ORDER BY updated_at desc) as row_num_actualizacion
      FROM `${project_source_2}.onboarding_savings.users` a
      LEFT JOIN `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_0001_Params` b ON 1=1 
      LEFT JOIN actividad d ON a.id = d.user
      WHERE 
          register_status in ('FINISHED')
          AND onboarding_status in ('FINISHED', 'READY', 'CP_SENT')
          AND DATETIME(a.onboarding_date , "America/Santiago") <= Fecha_Fin_Analisis
          AND a.onboarding_date  IS NOT NULL
--           AND state in (4,7,8)
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
DROP TABLE IF EXISTS `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos`;
CREATE TABLE `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos` AS (
  SELECT 
  user
  ,Fecha_Fin_Analisis_DT
  ,periodo
  ,dt_ob as fecha_evento
  ,'[Backend] Onboarding bolsillo completado' as event_name
  ,CAST(null as FLOAT64) as balance_amount
  ,f_actividad_app
  FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj` 
);

-- ECONOMICS EVENTS
INSERT `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos`
SELECT DISTINCT 
  e.user
  ,Fecha_Fin_Analisis_DT
  ,periodo
  ,DATETIME(trx_timestamp, "America/Santiago") trx_timestamp
  ,'[Economics] ' || linea as event_name
  ,CAST(null as FLOAT64) as balance_amount
  ,f_actividad_app
FROM `${project_source_1}.economics.economics` e 
JOIN `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj`  u on u.user = e.user AND DATETIME(trx_timestamp, "America/Santiago") <= Fecha_Fin_Analisis
WHERE 
  e.fecha >= fecha_ob
  AND linea in ('cash_in_savings', 'cash_out_savings') ;

-- CIERRES DE CUENTA
-- INSERT `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos`
-- SELECT DISTINCT 
--   user
--   ,Fecha_Fin_Analisis_DT
--   ,periodo
--   ,fecha_cierre
--   ,'[Backend] Cierre de Cuenta' as event_name
--   ,CAST(null as FLOAT64) as balance_amount
--   ,f_actividad_app
-- FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj` 
-- WHERE 
--   datetime(fecha_cierre) <= Fecha_Fin_Analisis
--   and cuenta_cerrada is true;

--SALDO EN BOLSILLO
INSERT `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos`
WITH 
  target as (
    SELECT DISTINCT
      Fecha_Analisis as fecha
      ,CAST(Fecha_Analisis as TIMESTAMP) trx_timestamp
      ,'Saldo en Bolsillo' as nombre
      ,CAST(SUM(amount) AS INT64) as monto
      ,CONCAT('sb_', user,'_',Fecha_Analisis) trx_id 
      ,user 
      ,'aum_savings' linea
      ,'app' canal
      ,'n/a' as comercio
      , null as id_comercio
      ,CAST(null as STRING) as actividad_cd
      ,CAST(null as STRING) as actividad_nac_cd
      ,CAST(null AS FLOAT64) as codact 
      ,row_number() over (partition by CONCAT('sb_', user,'_',Fecha_Analisis) order by Fecha_Analisis desc) as row_no_actualizacion 
    FROM `${project_target}.bolsillo.daily_aum`
    GROUP BY 1,2,6
    ),
  economics_aum as(
    
    SELECT DISTINCT
      * EXCEPT(row_no_actualizacion)
    FROM target
    WHERE 
      TRUE
      AND row_no_actualizacion = 1
    )
    
  SELECT DISTINCT 
      a.user 
      ,Fecha_Fin_Analisis_DT
      ,periodo
      ,DATETIME_SUB(DATETIME_ADD(a.fecha, INTERVAL 1 DAY), INTERVAL 1 SECOND) trx_timestamp --corrección por falla formato en clevertap events
      ,'[Saldo] Registro' event_name
      ,CAST(monto as FLOAT64) as balance_amount
      ,f_actividad_app
  FROM economics_aum   a
  JOIN `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj` b ON a.user = b.user and a.fecha <= b.Fecha_Fin_Analisis_DT
  WHERE
    linea = 'aum_savings';


DROP TABLE IF EXISTS `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`;
CREATE TABLE `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo` AS (
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
      ,CASE WHEN event_name = '[Economics] cash_out_savings' THEN 1 ELSE 0 
       END AS ultimo_evento_periodo_es_retiro_bolsillo 
    FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos`
    ), 
  agg_data_periodo as (
    SELECT 
      user
      ,Fecha_Fin_Analisis_DT
      ,periodo_orden
      ,coalesce(max(if(event_name = '[Backend] Onboarding bolsillo completado', 1, 0)), 0) as onboarding_periodo
      ,coalesce(max(if(event_name = '[Economics] cash_out_savings', 1, 0)), 0) as retiro_en_periodo
    FROM data
    GROUP BY 
      1,2,3
      ), 
  balance_data as (
    SELECT
      * except(balance_amount)
      , last_value(balance_amount ignore nulls) OVER (PARTITION BY user, periodo_orden ORDER BY fecha_evento rows between unbounded preceding and unbounded following)  as balance_amount
      --,coalesce(balance_amount, last_value(balance_amount ignore nulls) over (PARTITION BY user ORDER BY fecha_evento rows between unbounded preceding and unbounded following)) as balance_amount
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

INSERT INTO `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`
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
    FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_PubObj`
    LEFT JOIN (SELECT DISTINCT periodo_orden, periodo_legible FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`) ON 1=1
    WHERE 
       case when periodo = "monthly" then date_trunc(fecha_ob, month) 
       when periodo = "weekly" then date_trunc(fecha_ob, week) end <= date(periodo_legible)
      ) a 
LEFT JOIN `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo` b ON a.user = b.user and a.periodo_orden = b.periodo_orden
WHERE 
  b.user is null; 

DROP TABLE IF EXISTS `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux`;
CREATE TABLE `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo_Aux` AS (
   WITH 
    coalesced_data as (
      SELECT 
        *
        ,coalesce(balance_amount, last_value(balance_amount ignore nulls) over (PARTITION BY user  ORDER BY periodo_orden rows between unbounded preceding and current row), 0) as coalesced_balance_amount
        ,coalesce(f_actividad_app, last_value(f_actividad_app ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row)) as coalesced_f_actividad_app
        ,coalesce(periodo, last_value(periodo ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row)) as coalesced_periodo
        ,coalesce( ultimo_evento_periodo_es_retiro_bolsillo , last_value( ultimo_evento_periodo_es_retiro_bolsillo ignore nulls) over (PARTITION BY user ORDER BY periodo_orden rows between unbounded preceding and current row)) as coalesced_retiro_bolsillo
      FROM `${project_target}.temp.CHRN_BS_{{period}}_{{ds_nodash}}_102_Eventos_Periodo`
        ), 
    lagged_data as (
      SELECT 
        *
        ,lag(coalesced_retiro_bolsillo) over (PARTITION BY user ORDER BY periodo_orden) as retiro_bolsillo_periodo_pasado
      FROM coalesced_data
      ), 
    statet as (
      SELECT
        *
        ,CASE 
          WHEN coalesce(ultimo_evento_periodo_es_retiro_bolsillo, retiro_bolsillo_periodo_pasado, 0) = 1 and coalesce(coalesced_balance_amount, 0) <= 1 then 'churneado'
          ELSE 'activo' 
          END AS state
      FROM lagged_data
      ),
    last_statet as (
      SELECT
        *
        ,coalesce(lag(state) over (partition by user order by periodo_orden), 'onboarding') as last_state
      FROM statet
      )

  SELECT
    *
  FROM last_statet
);