{{ 
  config(
    materialized='table', 
    tags=["daily"],
  ) 
}}

with tickets_fresh_desk as (
    SELECT
        user,
        DATE_TRUNC(creacion, month) as month_creation,
    FROM `tenpo-bi-prod.external.tickets_freshdesk` 
    WHERE LENGTH(user) > 1 
    AND creacion >= '2021-10-01'
),
    --- usuarios con uno o mas tickets cada mes
    tickets_mes as (
        select distinct user, month_creation from tickets_fresh_desk
),

    trx_mes AS (
        SELECT
            user,
            DATE_TRUNC(fecha, month) as fecha_month,
        FROM `tenpo-bi-prod.economics.economics`
        WHERE linea <> 'reward' --and nombre not like '%Devol%'
        AND fecha >= '2021-10-01'
),

    MAUS AS (
        select distinct user, fecha_month from trx_mes
),
    churn_analysis AS (
        SELECT
            user, 
            DATE_TRUNC(Fecha_Fin_Analisis_DT, month) as fecha_month,
        FROM `tenpo-bi-prod.churn.daily_churn`
        WHERE churn=true
        AND Fecha_Fin_Analisis_DT >= '2021-10-01'
),
    churn_user as (
        select distinct user, fecha_month from churn_analysis
),  --usuarios con ticket y son mau en mes x
    TICKET_MAUS_mes1 as (
        select
            a.user, fecha_month, DATE_ADD(DATE_TRUNC(fecha_month, month), INTERVAL 1 MONTH) as fecha_month_2
        from  MAUS a
        INNER JOIN tickets_mes b ON a.user=b.user and DATE(a.fecha_month)=DATE(b.month_creation)
),  -- usuarios con ticket y son mau en mes x, Ademas son churn en mes 2
    user_kp1_2 as (
        select A.user, A.fecha_month from TICKET_MAUS_mes1 A
        LEFT JOIN churn_user B on A.user=B.user and A.fecha_month_2=B.fecha_month
        WHERE B.user IS NOT NULL
)

select fecha_month, count(*) as cantidad from user_kp1_2
group by fecha_month
order by fecha_month DESC