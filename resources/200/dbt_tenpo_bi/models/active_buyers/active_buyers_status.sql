{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
    cluster_by = "last_ndd_service",
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}

WITH
  data AS (
    SELECT
      DATE(u.ob_completed_at, "America/Santiago") AS fecha,
      COUNT(DISTINCT u.id) AS cuenta,
      last_ndd_service_bm last_ndd_service
    FROM {{ ref('users_tenpo') }}   u
    JOIN {{ ref('users_allservices') }}   a ON a.id = u.id
    WHERE 
      u.state in (4,7,8,21,22) 
      AND DATE(u.ob_completed_at, "America/Santiago") IS NOT NULL
      AND DATE(u.ob_completed_at, "America/Santiago") >= '2020-02-01'
    GROUP BY 1,3 
    ),
  active_buyers as (
    SELECT DISTINCT
      Fecha_Fin_Analisis_DT fecha_max,
      last_ndd_service_bm last_ndd_service,
      COALESCE(ma.source,'Nuevos') as migracion_atribuida,
      COUNT( DISTINCT user) as total_ab,
      COUNT( DISTINCT IF(linea = 'mastercard', user, null)) AS total_mc,
      COUNT( DISTINCT IF(linea = 'mastercard_physical', user, null)) AS total_mc_ph,
      COUNT( DISTINCT IF(linea = 'p2p', user, null)) AS total_p2p,
      COUNT( DISTINCT IF(linea = 'p2p_received', user, null)) AS total_p2p_cobro,
      COUNT( DISTINCT IF(linea = 'utility_payments', user, null)) AS total_up,
      COUNT( DISTINCT IF(linea = 'top_ups', user, null)) AS total_tu,
      COUNT( DISTINCT IF(linea = 'cash_in', user, null)) AS total_ci,
      COUNT( DISTINCT IF(linea = 'cash_out', user, null)) AS total_co,
      COUNT( DISTINCT IF(linea = 'paypal', user, null)) AS total_paypal,
      COUNT( DISTINCT IF(linea = 'crossborder', user, null)) AS total_crossborder,
      COUNT( DISTINCT IF(linea in ('cash_in_savings', 'cash_out_savings', 'aum_savings'), user, null)) AS total_bolsillo,
      COUNT( DISTINCT IF(linea in ('withdrawal_tyba', 'investment_tyba', 'aum_tyba'), user, null)) AS total_tyba,
    FROM  {{source('productos_tenpo','tenencia_productos_tenpo')}}
    JOIN {{ ref('users_allservices') }} ON user = id
    JOIN {{ ref('economics') }}  USING(user)
    LEFT JOIN{{ ref('migracion_atribuida') }} ma ON user = customer_user_id
    WHERE 
      fecha >=  date_sub(Fecha_Fin_Analisis_DT,interval 29 day)  
      AND fecha <= Fecha_Fin_Analisis_DT
      AND lower(linea) not in ({{ "'"+(var('not_in_lineas') | join("','"))+"'" }})
    GROUP BY
        1,2,3
    ),   
  churn as (
    SELECT
     Fecha_Fin_Analisis_DT fecha,
     last_ndd_service_bm last_ndd_service,
     COUNT(DISTINCT user) as cuenta,
    FROM  {{ ref('daily_churn') }}
    LEFT JOIN{{ ref('users_allservices') }} ON user = id
    WHERE 
      churn is true
    GROUP BY 
        1,2
    ),       
  dates AS (
      SELECT CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha
      FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2017-10-01'), CURRENT_DATE(), INTERVAL 1 DAY)) dia
      ),    
  services AS(  
     ( SELECT CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha, 'Nuevos' last_ndd_service  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2017-10-22'), CURRENT_DATE(), INTERVAL 1 DAY)) dia  ORDER BY dia DESC) union all
     ( SELECT CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha, 'Paypal' last_ndd_service  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2017-10-22'), CURRENT_DATE(), INTERVAL 1 DAY)) dia  ORDER BY dia DESC ) union all
     ( SELECT CAST(FORMAT_DATE('%Y-%m-%d', dia) as DATE) fecha, 'Topups' last_ndd_service  FROM UNNEST(GENERATE_DATE_ARRAY(DATE('2017-10-22'), CURRENT_DATE(), INTERVAL 1 DAY)) dia  ORDER BY dia DESC) 
      ),
  churn_acum as (
      SELECT 
        fecha,
        IF(cuenta is null, 0 , cuenta) cuenta ,
        last_ndd_service, 
        SUM(cuenta) OVER (ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS churn_acumulado  
      FROM churn 
      RIGHT JOIN services USING(fecha,last_ndd_service)
      ),
  ob_acum as (
      SELECT 
       fecha, 
       IF(cuenta is null, 0 , cuenta) cuenta ,
       last_ndd_service, 
       SUM(cuenta) OVER (ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ob_acumulado  
      FROM data 
      RIGHT JOIN services USING(fecha,last_ndd_service)
      ),
  ob_acum_proce as (  
     SELECT 
        fecha,
        last_ndd_service,
        cuenta as ob , 
        ob_acumulado, 
        SUM(cuenta) OVER (PARTITION BY last_ndd_service ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ob_acum_proced,
     FROM ob_acum ),
  summary as (
      SELECT  
            a.*,
            cuenta as churn , 
            churn_acumulado, 
            SUM(cuenta) OVER (PARTITION BY a.last_ndd_service  ORDER BY fecha ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS churn_acum_proced,
            ob,
            ob_acumulado,
            ob_acum_proced,
          FROM churn_acum   
      LEFT JOIN ob_acum_proce b USING(fecha,last_ndd_service)    
      RIGHT JOIN active_buyers  a  ON fecha = a.fecha_max AND a.last_ndd_service= b.last_ndd_service 
      WHERE 
        a.last_ndd_service  is not null
        )
SELECT
  fecha_max fecha,
  * EXCEPT(fecha_max),
  (ob_acum_proced-churn_acum_proced) clientes_acum_proced,
  SAFE_DIVIDE(total_ab,(ob_acum_proced-churn_acum_proced)) ratio,
  SAFE_DIVIDE(total_mc,(ob_acum_proced-churn_acum_proced)) ratio_mc,
  SAFE_DIVIDE(total_mc_ph,(ob_acum_proced-churn_acum_proced)) ratio_mc_ph,
  SAFE_DIVIDE(total_p2p,(ob_acum_proced-churn_acum_proced)) ratio_p2p,
  SAFE_DIVIDE(total_p2p_cobro, (ob_acum_proced-churn_acum_proced)) ratio_p2p_cobro,
  SAFE_DIVIDE(total_up,(ob_acum_proced-churn_acum_proced)) ratio_up,
  SAFE_DIVIDE(total_tu,(ob_acum_proced-churn_acum_proced)) ratio_tu,
  SAFE_DIVIDE(total_ci,(ob_acum_proced-churn_acum_proced)) ratio_ci,
  SAFE_DIVIDE(total_co,(ob_acum_proced-churn_acum_proced)) ratio_co,
  SAFE_DIVIDE(total_paypal,(ob_acum_proced-churn_acum_proced)) ratio_paypal,
  SAFE_DIVIDE(total_crossborder,(ob_acum_proced-churn_acum_proced)) ratio_crossborder,
  SAFE_DIVIDE(total_bolsillo,(ob_acum_proced-churn_acum_proced)) ratio_bolsillo,
  SAFE_DIVIDE(total_tyba,(ob_acum_proced-churn_acum_proced)) ratio_tyba,
  SAFE_DIVIDE(total_ab,(ob_acum_proced)) ratio_ob,
  SAFE_DIVIDE(total_mc,(ob_acum_proced)) ratio_mc_ob,
  SAFE_DIVIDE(total_mc_ph,(ob_acum_proced)) ratio_mc_ph_ob,
  SAFE_DIVIDE(total_p2p,(ob_acum_proced)) ratio_p2p_ob,
  SAFE_DIVIDE(total_p2p_cobro, (ob_acum_proced)) ratio_p2p_cobro_ob,
  SAFE_DIVIDE(total_up,(ob_acum_proced)) ratio_up_ob,
  SAFE_DIVIDE(total_tu,(ob_acum_proced)) ratio_tu_ob,
  SAFE_DIVIDE(total_ci,(ob_acum_proced)) ratio_ci_ob,
  SAFE_DIVIDE(total_co,(ob_acum_proced)) ratio_co_ob,
  SAFE_DIVIDE(total_paypal,(ob_acum_proced)) ratio_paypal_ob,
  SAFE_DIVIDE(total_crossborder,(ob_acum_proced)) ratio_crossborder_ob,
  SAFE_DIVIDE(total_bolsillo,(ob_acum_proced)) ratio_bolsillo_ob,
  SAFE_DIVIDE(total_tyba,(ob_acum_proced)) ratio_tyba_ob,
FROM summary