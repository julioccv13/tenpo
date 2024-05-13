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
  ), publico_objetivo as (
    SELECT DISTINCT
      id
    FROM {{ ref('users_tenpo') }} --`tenpo-bi-prod.users.users_tenpo`
    WHERE
      state in (4,7,8,21,22)
  ), economics as (
    SELECT DISTINCT
      fecha
      ,DATE_TRUNC(fecha, MONTH) as mes
      ,trx_timestamp
      ,linea
      ,trx_id
      ,nombre
      ,a.user
      ,monto
    FROM {{ ref('economics') }} a --`tenpo-bi-prod.economics.economics` -- USING(user)
    JOIN  {{ ref('users_tenpo') }} b on b.id = a.user --`tenpo-bi-prod.users.users_tenpo`
    INNER join publico_objetivo c on c.id = b.id
    WHERE true
      AND linea not in ('reward')
      AND nombre not like "%Devolución%"
  ), churn_summary as (
      SELECT distinct
      Fecha_Fin_Analisis_DT
      ,DATE_TRUNC(a.Fecha_Fin_Analisis_DT, MONTH) as mes
      ,mau_type
      ,count(distinct a.user) usuarios
      ,count(distinct case when state != "cuenta_cerrada" then a.user else null end) as cliente
      ,count(distinct case when state = "activo" then a.user else null end) clientes_ready 
      FROM {{ ref('daily_churn') }} a
      left join {{ ref('mau_type_per_month') }} b 
      on a.user = b.user and DATE_TRUNC(a.Fecha_Fin_Analisis_DT, MONTH) = b.mes
      where Fecha_Fin_Analisis_DT = LEAST(LAST_DAY(Fecha_Fin_Analisis_DT), (SELECT MAX(Fecha_Fin_Analisis_DT) FROM {{ ref('daily_churn') }}))

      GROUP BY  
        1,2,3
  ), calculo_maus as (
      SELECT DISTINCT
        a.mes
        ,mau_type
        ,COUNT( DISTINCT a.user) as maus
        ,COUNT( distinct case when linea in ('mastercard','mastercard_physical')  then a.user else null end ) mau_mc
        ,COUNT( distinct case when linea in ('utility_payments')  then a.user else null end ) mau_up
        ,COUNT( distinct case when linea in ('top_ups')  then a.user else null end ) mau_tu
        ,COUNT( distinct case when linea in ('paypal')  then a.user else null end ) mau_pp
        ,COUNT( distinct case when linea in ('crossborder')  then a.user else null end ) mau_cb
        ,COUNT( distinct case when linea like '%p2p%'  then a.user else null end ) mau_p2p
        ,COUNT( distinct case when linea in ('cash_in_savings', 'cash_out_savings', 'aum_bolsillo')  then a.user else null end ) mau_bs
        ,COUNT( distinct case when linea in ('investment_tyba')  then a.user else null end ) mau_tyba
        ,COUNT( distinct case when linea in ('cash_in')  then a.user else null end ) mau_ci
        ,COUNT( distinct case when linea in ('cash_out')  then a.user else null end ) mau_co
        ,SUM(case when linea in ('mastercard','mastercard_physical', 'utility_payments', 'top_ups', 'paypal','crossborder') and nombre not like "%Devolución%" then monto else 0 end ) gpv
        ,COUNT( distinct case when linea in ('mastercard','mastercard_physical', 'utility_payments', 'top_ups', 'paypal','crossborder') and nombre not like "%Devolución%" then trx_id else null end ) trx
      FROM economics a
      left join {{ ref('mau_type_per_month') }} b 
      on a.user = b.user and a.mes = b.mes
      GROUP BY 
        1, 2
  ), aum as (
      SELECT 
        DISTINCT
        Fecha_Analisis,
        DATE_TRUNC(a.fecha_analisis, MONTH) as mes,
        b.mau_type,
        SUM(amount)  aum
      FROM {{source('bolsillo','daily_aum')}} a --`tenpo-bi.bolsillo.daily_aum`  -- USING(user)
      left join {{ ref('mau_type_per_month') }} b 
      on a.user = b.user and DATE_TRUNC(a.fecha_analisis, MONTH) = b.mes
      where Fecha_Analisis = LAST_DAY(Fecha_Analisis)
      GROUP BY 
        1, 2, 3
    ), promedio_visitas_mes as (
    SELECT 
      mes_visita mes
      ,mau_type
      ,SUM(visitas_totales ) visitas_totales
      ,SUM(visitas_totales )/COUNT(distinct tenpo_uuid) visitas_prom
      ,SUM(case when f_mau is true then visitas_totales else null end) visitas_totales_mau
      ,SUM(case when f_mau is true then visitas_totales else null end)/COUNT(distinct case when f_mau is true then tenpo_uuid else null end) visitas_prom_mau
    FROM {{ ref('sessions_by_month_by_user') }} A --`tenpo-bi-prod.activity.sessions_by_month_by_user` 
      left join {{ ref('mau_type_per_month') }} b 
      on B.user = A.tenpo_uuid and a.mes_visita = b.mes
    GROUP BY 
      1, 2
  ), saldos as (
      SELECT
       a.fecha
       ,a.user
       ,date_trunc(fecha, month) as mes
       ,mau_type
       ,saldo_dia  
      FROM {{ ref('daily_balance') }} a
      left join {{ ref('mau_type_per_month') }} b 
      on a.user = b.user and DATE_TRUNC(a.fecha, MONTH) = b.mes
  ), saldo_mes as (
        SELECT
          fecha
          ,mau_type
          ,mes
          ,sum(saldo_dia) saldo_total
          ,sum(case when saldo_dia > 0 then saldo_dia else null end) saldo_positivo_total
          ,count(case when saldo_dia > 0 then saldo_dia else null end) con_saldo_positivo
          ,avg(case when saldo_dia > 0 then saldo_dia else null end) saldo_promedio
        FROM saldos
        where fecha = LAST_DAY(fecha)
        GROUP BY 
          1, 2, 3

  ), daily_churn as (
      SELECT distinct
        a.Fecha_Fin_Analisis_DT
        ,date_trunc(Fecha_Fin_Analisis_DT, month) as mes
        ,a.user
        ,mau_type
        ,not churn as f_cliente_ready
      FROM {{ ref('daily_churn') }} a
      left join {{ ref('mau_type_per_month') }} b 
      on a.user = b.user and DATE_TRUNC(a.Fecha_Fin_Analisis_DT, MONTH) = b.mes
      where Fecha_Fin_Analisis_DT = LAST_DAY(Fecha_Fin_Analisis_DT)
  ), ratio_cliente_ready_saldo as (
    SELECT
      mes 
      ,mau_type
      ,COUNT(distinct user) as saldo_users
      ,count( distinct if(f_saldo, user, null)) as con_saldo
      ,count( distinct if(f_saldo, user, null))/COUNT(distinct user) ratio_c_ready_saldo
    FROM(
      SELECT
        a.mes
        ,a.user
        ,b.mau_type
        ,(b.saldo_dia is not null and b.saldo_dia > 0) as f_saldo
      FROM daily_churn a
      LEFT JOIN saldos b 
      on a.user = b.user and a.Fecha_Fin_Analisis_DT = b.fecha -- a.fecha_fin_analisis_dt es ult. fecha del mes, fyi
      WHERE 
        f_cliente_ready is TRUE
    )
    GROUP BY 
      1, 2
  ), last_tarjeta as (
    SELECT
    * except(fecha)
    ,date_trunc(fecha, month) as mes
    FROM {{ref('daily_activation_ratio_by_mau_type')}}
    where fecha = LAST_DAY(fecha)
  )

            
  SELECT 
    a.mes
    ,b.mau_type

    ,b.* EXCEPT(Fecha_Fin_Analisis_DT, mes, mau_type)
    ,c.* EXCEPT(mes, mau_type)

    ,(c.mau_bs+c.mau_cb+c.mau_mc+c.mau_p2p+c.mau_pp+c.mau_tu +c.mau_up+mau_tyba)/maus as tenencia_promedio
    ,(c.mau_bs+c.mau_cb+c.mau_mc+c.mau_p2p+c.mau_pp+c.mau_tu +c.mau_up+mau_tyba) as productos
    ,(c.mau_bs+c.mau_cb+c.mau_mc+c.mau_p2p+c.mau_pp+c.mau_tu +c.mau_up+mau_tyba+c.mau_ci+c.mau_co) as productos_cico
    ,d.aum

    ,e.* EXCEPT(mes, mau_type)

    ,f.saldo_total
    ,f.saldo_positivo_total
    ,f.saldo_promedio
    ,f.con_saldo_positivo

    ,g.ratio_c_ready_saldo
    ,g.con_saldo
    ,g.saldo_users
    
    ,h.* except (mes, mau_type)
  FROM tabla_base a
  LEFT JOIN churn_summary b on b.mes = a.mes
  LEFT JOIN calculo_maus c on c.mes = b.mes and b.mau_type = c.mau_type
  LEFT JOIN aum d on d.mes = b.mes and b.mau_type = d.mau_type
  LEFT JOIN promedio_visitas_mes e on e.mes = b.mes and b.mau_type = e.mau_type
  LEFT JOIN saldo_mes f on f.mes = b.mes and b.mau_type = f.mau_type
  LEFT JOIN ratio_cliente_ready_saldo g on g.mes = b.mes and b.mau_type = g.mau_type
  LEFT JOIN last_tarjeta h on h.mes = b.mes and b.mau_type = h.mau_type