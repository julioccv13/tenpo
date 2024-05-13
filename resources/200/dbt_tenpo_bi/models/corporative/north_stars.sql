{{ 
  config(
    tags=["daily", "bi"],
    materialized='table', 
  ) 
}}

WITH 
  tabla_base as (
        SELECT distinct
          DATE_TRUNC(dia, month) mes
          ,DATE_SUB(DATE_ADD(DATE_TRUNC(dia, month), INTERVAL 1 MONTH), interval 1 day) ult_dia_mes
        FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', current_date("America/Santiago"), INTERVAL 1 MONTH)) AS dia
       ),
  publico_objetivo as (
    SELECT DISTINCT
      id
    FROM {{ ref('users_tenpo') }} --`tenpo-bi-prod.users.users_tenpo`
    WHERE
      state in (4,7,8,21,22)
      ),
  daily_churn as (
      SELECT distinct
        Fecha_Fin_Analisis_DT
        ,user
        ,case when churn is false then true else false end as f_cliente_ready
      FROM {{ ref('daily_churn') }} 
  ),
  churn_summary as (
      SELECT
        Fecha_Fin_Analisis_DT 
        , usuarios
        , cliente 
        , clientes_ready 
      FROM {{ ref('churn_daily_summary') }} --`tenpo-bi-prod.churn.churn_daily_summary`
      ),
  tarjetas as (
      SELECT
        fecha
        ,tarjetas_activadas
        ,cuenta_physical
        ,cuenta_virtual
        ,cuenta_clientes_tarjeta
      FROm {{ ref('daily_activation_ratio') }}  --`tenpo-bi-prod.prepago.monthly_activation_ratio` 
     ),
  maus as (
    SELECT DISTINCT
      fecha
      ,trx_timestamp
      ,linea
      ,trx_id
      ,nombre
      ,user
      ,monto
    FROM {{ ref('economics') }} --`tenpo-bi-prod.economics.economics` -- USING(user)
    JOIN  {{ ref('users_tenpo') }} on id = user --`tenpo-bi-prod.users.users_tenpo`
    WHERE true
      AND linea not in ('reward')
      AND nombre not like "%Devolución%"
       ),
  calculo_maus as (
    SELECT DISTINCT
      DATE_TRUNC(fecha, MONTH) as mes
      ,COUNT( DISTINCT user) as maus
      ,COUNT( distinct case when linea in ('mastercard','mastercard_physical')  then user else null end ) mau_mc
      ,COUNT( distinct case when linea in ('utility_payments')  then user else null end ) mau_up
      ,COUNT( distinct case when linea in ('top_ups')  then user else null end ) mau_tu
      ,COUNT( distinct case when linea in ('paypal')  then user else null end ) mau_pp
      ,COUNT( distinct case when linea in ('crossborder')  then user else null end ) mau_cb
      ,COUNT( distinct case when linea like '%p2p%'  then user else null end ) mau_p2p
      ,COUNT( distinct case when linea in('cash_in')  then user else null end ) mau_ci
      ,COUNT( distinct case when linea in('cash_out')  then user else null end ) mau_co
      ,COUNT( distinct case when linea in ('cash_in_savings', 'cash_out_savings', 'aum_bolsillo')  then user else null end ) mau_bs
      ,SUM(case when linea in ('mastercard','mastercard_physical', 'utility_payments', 'top_ups', 'paypal','crossborder') and nombre not like "%Devolución%" then monto else 0 end ) gpv
      ,COUNT( distinct case when linea in ('mastercard','mastercard_physical', 'utility_payments', 'top_ups', 'paypal','crossborder') and nombre not like "%Devolución%" then trx_id else null end ) trx
    FROM maus
    GROUP BY 
      1
      ),
    aum as (
    SELECT DISTINCT
      Fecha_Analisis,
      SUM(amount)  aum
    FROM {{source('bolsillo','daily_aum')}} --`tenpo-bi.bolsillo.daily_aum`  -- USING(user)
    WHERE true
    GROUP BY 
      1
    ),
  promedio_visitas_mes as (
    SELECT 
      mes_visita mes
      ,SUM(visitas_totales ) visitas_totales
      ,SUM(visitas_totales )/COUNT(distinct tenpo_uuid) visitas_prom
      ,SUM(case when f_mau is true then visitas_totales else null end) visitas_totales_mau
      ,SUM(case when f_mau is true then visitas_totales else null end)/COUNT(distinct case when f_mau is true then tenpo_uuid else null end) visitas_prom_mau
    FROM {{ ref('sessions_by_month_by_user') }} --`tenpo-bi-prod.activity.sessions_by_month_by_user` 
    GROUP BY 
      1
      ),
  saldos as (
      SELECT
       fecha
       ,user
       ,saldo_dia  
      FROM {{ ref('daily_balance') }}
        ),
  saldo_mes as (
        SELECT
          fecha
          ,sum(saldo_dia) saldo_total
          ,avg(case when saldo_dia > 0 then saldo_dia else null end) saldo_promedio
        FROM saldos
        GROUP BY 
          1
        ),
  ratio_cliente_ready_saldo as (
    SELECT
      Fecha_Fin_Analisis_DT 
      ,count( distinct case when f_saldo is true then user else null end)/COUNT(distinct user) ratio_c_ready_saldo
    FROM(
      SELECT
        a.Fecha_Fin_Analisis_DT
        ,a.user
        ,case when b.saldo_dia is null then false else true end as f_saldo
      FROM daily_churn a
      LEFT JOIN saldos b on a.user =b.user and a.Fecha_Fin_Analisis_DT = b.fecha
      WHERE 
        f_cliente_ready is TRUE
        ) 
    GROUP BY 
      1
    ORDER BY
      1 desc
      )

            
    SELECT 
      a.mes
      ,b.* EXCEPT(Fecha_Fin_Analisis_DT)
      ,c.* EXCEPT(mes)
      ,(c.mau_bs+c.mau_cb+c.mau_mc+c.mau_p2p+c.mau_pp+c.mau_tu +c.mau_up)/maus tenencia_promedio
      ,(c.mau_bs+c.mau_cb+c.mau_mc+c.mau_p2p+c.mau_pp+c.mau_tu +c.mau_up+c.mau_ci+c.mau_co)/maus tenencia_promedio_cico
      ,d.aum
      ,e.* EXCEPT(mes)
      ,f.saldo_total
      ,f.saldo_promedio
      ,g.ratio_c_ready_saldo
      ,h.* EXCEPT(fecha)
    FROM tabla_base a
    LEFT JOIN churn_summary b on b.Fecha_Fin_Analisis_DT = a.ult_dia_mes
    LEFT JOIN calculo_maus c on c.mes = a.mes
    LEFT JOIN aum d on d.Fecha_Analisis = a.ult_dia_mes 
    LEFT JOIN promedio_visitas_mes e on e.mes = a.mes
    LEFT JOIN saldo_mes f on f.fecha = a.ult_dia_mes
    LEFT JOIN ratio_cliente_ready_saldo g on g.Fecha_Fin_Analisis_DT = a.ult_dia_mes
    LEFT JOIN tarjetas h on h.fecha= a.ult_dia_mes
    ORDER BY 
      1 DESC