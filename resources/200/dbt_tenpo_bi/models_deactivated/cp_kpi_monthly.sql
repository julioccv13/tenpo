{{ config(materialized='table',  tags=["daily", "bi"]) }}

WITH 
  ----------------------------------------------------
  -->>                 VALORES REALES             <<--
  ----------------------------------------------------   
  tabla_metricas as (
    SELECT
       dia, mes, linea, tipo_usuario, canal,
       [
           STRUCT('gpv_clp' AS metric, gpv_clp AS value),
           STRUCT('gpv_usd' AS metric, gpv_usd AS value),
           STRUCT('trx' AS metric, trx AS value),
           STRUCT('usr' AS metric, usr AS value)
       ] AS economic
    FROM (
          -----------------------------------
          --          PAYPAL-WEB           --
          -----------------------------------
          SELECT
             MAX(DATE(fec_fechahora_envio , "America/Santiago")) AS dia,
             FORMAT_DATE('%Y-%m-01', DATE(fec_fechahora_envio , "America/Santiago")) mes,
             SUM(CAST(mto_monto_dolar as FLOAT64) * CAST(valor_dolar_multicaja AS FLOAT64)) as gpv_clp,
             SUM(CAST(mto_monto_dolar AS FLOAT64)) as gpv_usd,
             COUNT(codigo_mc) as trx,
             COUNT(DISTINCT(id_cuenta))as usr,
             'paypal' as linea,
             'web' as canal,
             CASE 
               WHEN tip_trx  = "ABONO_PAYPAL" THEN 'abono'
               ELSE 'retiro' 
               END AS tipo_usuario,
           FROM {{source('paypal','pay_transaccion')}}
           WHERE 
            est_estado_trx IN (2,3,8,17,24) 
            AND tip_trx IN ("ABONO_PAYPAL","RETIRO_PAYPAL","RETIRO_USD_PAYPAL")
            AND DATE(fec_fechahora_envio)< (SELECT MAX(DATE(fec_fechahora_envio)) FROM {{source('paypal','pay_transaccion')}})
           GROUP BY mes, tipo_usuario
          UNION ALL 
          -----------------------------------
          --          TOPUPBS-WEB          --
          -----------------------------------             
          (
          SELECT DISTINCT
            MAX(DATE( t.fecha_creacion , "America/Santiago")) AS dia,
            FORMAT_DATE('%Y-%m-01', DATE(t.fecha_creacion  ,"America/Santiago")) mes,
            SUM(t.monto) as gpv_clp,
            SUM(t.monto / valor_dolar_cierre) as gpv_usd,
            COUNT(distinct t.id) as trx,
            COUNT(DISTINCT IF(t.id_usuario  > 0, CAST (t.id_usuario AS STRING), (IF(t.id_usuario < 0 AND t.correo_usuario <> "",t.correo_usuario,r.suscriptor)))) AS usr,
            'top_ups' as linea,
            'web' as canal,
            'generico' tipo_usuario,
          FROM (select distinct * from {{source('topups_web','ref_transaccion')}})  t
          JOIN (select distinct * from {{source('topups_web','ref_recarga')}}) r ON t.id = r.id_transaccion
          JOIN (select distinct * from {{source('topups_web','ref_producto')}}) p ON p.id = r.id_producto 
          JOIN (select distinct * from {{source('topups_web','ref_operador')}}) o ON o.id = p.id_operador
          JOIN (select distinct * from {{source('topups_web','ref_comisiones')}}) c ON c.id_producto = p.id 
          JOIN (select distinct * from {{source('topups_web','ref_tipo_producto')}}) tp ON tp.id = p.id_tipo_producto
          JOIN (select distinct * from {{ ref('dolar') }})   ON DATE( t.fecha_creacion , "America/Santiago") = fecha
          WHERE 
            t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5)
          GROUP BY 
            mes, tipo_usuario 
          )
          UNION ALL 
          -----------------------------------
          --          TENPO-APP            --
          -----------------------------------   
          (
          SELECT
            MAX(e.fecha) AS dia,
            FORMAT_DATE('%Y-%m-01', e.fecha) mes,
            SUM(monto) as gpv_clp,
            SUM(monto / valor_dolar_cierre) as gpv_usd,
            COUNT(DISTINCT trx_id) as trx,
            COUNT(DISTINCT user) as usr,
            linea,
            'app' as canal,
            'generico' as tipo_usuario
          FROM {{ ref('economics') }} e
          JOIN {{ ref('dolar') }} d ON e.fecha = d.fecha
          WHERE 
            linea in ('mastercard', 'utility_payments', 'top_ups', 'p2p','crossborder')
          GROUP BY mes, tipo_usuario, linea 
          
          UNION ALL

          SELECT
            MAX(e.fecha) AS dia,
            FORMAT_DATE('%Y-%m-01', e.fecha) mes,
            SUM(monto) as gpv_clp,
            SUM(monto / valor_dolar_cierre) as gpv_usd,
            COUNT(DISTINCT trx_id) as trx,
            COUNT(DISTINCT user) as usr,
            linea,
            'app' as canal,
            'generico' as tipo_usuario
          FROM {{ ref('economics') }} e
          JOIN {{ ref('dolar') }} d ON e.fecha = d.fecha
          WHERE 
            linea in ('paypal')
            AND e.fecha< (SELECT MAX(e.fecha) FROM {{ ref('economics') }} e WHERE linea in ('paypal'))
          GROUP BY mes, tipo_usuario, linea 

          )
         -----------------------------------
         --          BOLSILLO            --
         -----------------------------------             
         UNION ALL        
         (
          SELECT
            MAX(e.fecha) AS dia,
            FORMAT_DATE('%Y-%m-01', e.fecha) mes,
            SUM(IF(linea = 'cash_in_savings', monto, null)) as gpv_clp,
            SUM(IF(linea = 'cash_in_savings', monto, null) / valor_dolar_cierre) as gpv_usd,
            COUNT(DISTINCT IF(linea in ('cash_in_savings', 'cash_out_savings'), trx_id, null)) as trx,
            COUNT(DISTINCT user) as usr,
            'bolsillo' linea,
            'app' as canal,
            'generico' as tipo_usuario
          FROM {{ ref('economics') }} e
          JOIN {{ ref('dolar') }} d ON e.fecha = d.fecha
          WHERE 
            linea in ('aum_savings', 'cash_out_savings', 'cash_in_savings')
          GROUP BY mes, tipo_usuario, linea 
          )
        )
       ),
  economics as (
    SELECT
      CASE WHEN metrica.metric  = 'gpv_clp' THEN 'gpv'
       WHEN metrica.metric  = 'gpv_usd' THEN 'gpv'
       ELSE metrica.metric 
       END AS categoria,
      CASE WHEN metrica.metric  = 'gpv_clp' THEN 'clp'
       WHEN metrica.metric  = 'gpv_usd' THEN 'usd'
       ELSE 'clp' 
       END AS moneda,
      CAST(metrica.value AS INT64) as valor,
      linea,
      tipo_usuario,
      canal,
      dia as fecha,
      mes
    FROM
      tabla_metricas
     CROSS JOIN
        UNNEST(tabla_metricas.economic) AS metrica
 ),
 real as (
   SELECT
     categoria,
     moneda,
     SUM(valor) valor,
     linea,
     tipo_usuario,
     canal,
     fecha,
     FORMAT_DATE('%Y-%m-01', fecha) mes,
   FROM  economics
   GROUP BY
    categoria,linea, tipo_usuario, canal, moneda, mes, fecha
     ),
  ----------------------------------------------------
  -->>                 PRESUPUESTO                <<--
  ----------------------------------------------------   
  presupuesto as (
      select * from {{ref('presupuesto_mensual')}}
        ),
  ----------------------------------------------------
  -->>     UNIÓN PRESUPUESTO + VALORES REALES     <<--
  ----------------------------------------------------          
  tabla_unificada as (
    SELECT
      categoria , 
      SUM(valor) valor_mes , 
      SUM(p.presupuesto) presupuesto_mes , 
      linea , 
      tipo_usuario , 
      canal , 
      mes , 
      moneda,
      EXTRACT(YEAR FROM CAST(mes AS DATE)) as year,
      IF( FORMAT_DATE('%Y-%m-01', CURRENT_DATE("America/Santiago")) = mes, 
       EXTRACT(DAY FROM DATE_SUB(DATE_TRUNC(DATE_ADD(max(r.fecha), INTERVAL 1 MONTH), MONTH), INTERVAL 1 DAY))/EXTRACT( DAY FROM DATE_ADD(max(r.fecha), INTERVAL 0 DAY)), 1) as multiplicador, 
    FROM(
       SELECT
        categoria , 
        SUM(valor) valor , 
        linea , 
        tipo_usuario , 
        canal , 
        mes , 
        fecha,
        moneda, 
       FROM real 
       GROUP BY 
        categoria,linea, tipo_usuario, canal, moneda, mes, fecha
        ) r
         FULL JOIN presupuesto p USING (categoria, linea, tipo_usuario, canal, mes, fecha, moneda)
     WHERE 
       linea is not null
     GROUP BY
       categoria,linea, tipo_usuario, canal, moneda, mes ), 

  calculo_ytd as (
    SELECT 
       t.*, 
       LAG(t.valor_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal ORDER BY t.mes) as last_month, 
       SUM(t.valor_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, t.year ORDER BY t.mes) as year_to_date,
       SUM(t.presupuesto_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, t.year ORDER BY t.mes) as acc_target
    FROM tabla_unificada t
    ),

  calculo_last_ytd as (
  SELECT 
    t.*, 
    LAG(t.year_to_date) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, EXTRACT(MONTH FROM CAST(mes AS DATE))  ORDER BY t.year) as last_year_to_date,
    LAG(t.valor_mes) OVER (PARTITION BY t.categoria, t.linea, t.tipo_usuario, t.moneda, t.canal, EXTRACT(MONTH FROM CAST(mes AS DATE))  ORDER BY t.year) as month_last_year
  FROM calculo_ytd t
  )

SELECT * FROM calculo_last_ytd 

