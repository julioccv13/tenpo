{{ config(tags=["daily", "bi"], materialized='table') }}

WITH  
 -- TODO: Pasar a tabla.
 rut_ids as (
   SELECT
    distinct
    id
    ,tributary_identifier
   FROM {{ source('tenpo_users', 'users') }}
 ),

  /*
    Este fix fue un parche rapido, aqui lo que paso es que han habido dos procesos de saldo
    que cargaron los datos en tablas diferentes, el cambio se hizo a partir del 22 de diciembre

    #TODO: CARGARLO TODO EN UNA TABLA
  */
 saldo as ( 
    SELECT DISTINCT 
      DATE(s.fecha) as fecha,
      u.id,
      s.SaldoConciliado saldo_dia
    FROM {{ source('tenpo_users', 'saldos') }}   s --`tenpo-airflow-prod.users.saldos`
    JOIN rut_ids u  ON u.tributary_identifier = s.rut_hash -- {{source('tenpo_users','users')}}u
    where s.fecha < '2021-12-22'
    ),

  saldo_v2 as ( 
    SELECT DISTINCT 
      DATE(s.fecha) as fecha,
      user as id,
      s.saldo_app saldo_dia
    FROM {{ ref('saldo_app_reconciliation') }}  s --`tenpo-airflow-prod.users.saldos`
    --JOIN rut_ids u  ON u.tributary_identifier = s.rut_hash -- {{source('tenpo_users','users')}}u
    WHERE s.fecha >= '2021-12-22'
    ),


 saldo_backup as (
   SELECT DISTINCT 
      fecha,
      u.id,
      s.saldo_conciliado saldo_dia
    FROM {{source('backup','backup_saldos_202106')}} s 
    JOIN rut_ids u   USING(tributary_identifier) --{{source('tenpo_users','users')}} u
    WHERE fecha not in (SELECT distinct fecha from saldo)
    ),
 union_saldos as (
    SELECT
      *
    FROM(
        SELECT* FROM saldo_backup
        UNION ALL
        SELECT * FROM saldo
        UNION ALL
        SELECT * FROM saldo_v2
        )
    WHERE TRUE
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY id,fecha order by fecha desc) = 1
    ),
 datos_tablon as (
   SELECT DISTINCT 
    user,
    LAST_VALUE( rfmp_frequency ) OVER ( PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) cuenta_trx_origen
   FROM {{ source('tablones_analisis', 'tablon_rfmp_v2') }}  
    ),
 churn as (
   SELECT DISTINCT
    user id,
    Fecha_Fin_Analisis_DT,
    churn
   FROM  {{ ref('daily_churn') }} --`tenpo-bi-prod.churn.churn_app`  
    ),
  tabla_base as (
    SELECT distinct
      dia fecha
      ,id user
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', current_date("America/Santiago"), INTERVAL 1 day)) AS dia
    CROSS JOIN (SELECT distinct id FROM union_saldos)
    )
    
    SELECT
       a.fecha
       ,a.user
       ,CASE WHEN b.saldo_dia is null then last_value(b.saldo_dia ignore nulls) over (partition by a.user order by a.fecha rows between unbounded preceding and current row ) else b.saldo_dia end as saldo_dia
       ,CASE WHEN cuenta_trx_origen > 0 THEN true ELSE false END AS bool_trx_origen,
       churn
    FROM tabla_base a
    LEFT JOIN union_saldos b on a.user = b.id and a.fecha = b.fecha
    LEFT JOIN churn c ON c.id = a.user and c.Fecha_Fin_Analisis_DT = a.fecha
    LEFT JOIN datos_tablon d ON d.user = a.user    
    WHERE TRUE
    QUALIFY 
      case when b.saldo_dia is null then last_value(b.saldo_dia ignore nulls) over (partition by a.user order by a.fecha rows between unbounded preceding and current row ) else b.saldo_dia end is not null