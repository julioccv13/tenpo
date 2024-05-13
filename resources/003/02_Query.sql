DROP TABLE IF EXISTS `{{project_id}}.temp.TPT_{{ds_nodash}}_102_Temp`;
CREATE TABLE `{{project_id}}.temp.TPT_{{ds_nodash}}_102_Temp` AS (

WITH     
    users AS (
        SELECT 
            id user, 
            email,
            Fecha_Fin_Analisis,
            Fecha_Fin_Analisis_DT,
            row_number() over (partition by id order by updated_at desc) as row_num_actualizacion,
            case when state in (7,8) then true else false end as cuenta_cerrada
        FROM
            `{{project_source_3}}.users.users` a
            LEFT JOIN `{{project_id}}.temp.TPT_{{ds_nodash}}_003_Params` b ON 1=1 
        WHERE 
            state in (4,7,8,21,22)
            and DATE(a.created_at, "America/Santiago") <= Fecha_Fin_Analisis_DT
    ), users_distinct as (
        SELECT 
            *
        FROM users
        WHERE row_num_actualizacion = 1
    ), economics AS (
        SELECT
          c.Fecha_Fin_Analisis
          ,fecha
          ,CAST(trx_timestamp AS DATETIME) trx_timestamp
          ,trx_id
          ,linea
          ,CASE 
            WHEN linea in ('p2p', 'p2p_received') THEN 'p2p' 
            WHEN linea in ('cash_in_savings', 'cash_out_savings') THEN 'bolsillo'
            WHEN linea in ('withdrawal_tyba', 'investment_tyba') THEN 'tyba' 
            WHEN linea like '%mastercard%' THEN 'mastercard'
            ELSE linea END linea_simplif
          ,b.user
          ,CAST(UNIX_SECONDS(trx_timestamp)/(60*60*24) AS INT64) as dias_registro_desde_epoch
          ,false as ward_de_analisis
        FROM `{{project_source_1}}.economics.economics` a
        JOIN users_distinct b ON b.user = a.user
        LEFT JOIN `{{project_id}}.temp.TPT_{{ds_nodash}}_003_Params` c ON 1=1
        WHERE 
          a.fecha <= c.Fecha_Fin_Analisis_DT 
            AND linea not in ('reward')
            AND nombre not like '%Devolución%'
        UNION ALL
        SELECT distinct
         Fecha_Fin_Analisis
         ,Fecha_Fin_Analisis
         ,Fecha_Fin_Analisis_DT
         ,CAST(null as STRING)
         ,CAST(null as STRING)
         ,CAST(null as STRING)
         ,user
         ,CAST(UNIX_SECONDS(cast(Fecha_Fin_Analisis_DT as timestamp))/(60*60*24) AS INT64) as dias_registro_desde_epoch
         ,true as ward_de_analisis
        FROM users
    ), datos as ( 
        SELECT 
          Fecha_Fin_Analisis
          ,user
          ,economics.fecha 
          ,trx_timestamp
          ,linea
          ,dias_registro_desde_epoch
          ,ward_de_analisis
          ,array_agg(case when ward_de_analisis is FALSE then linea else '__WARD__' end) over (partition by user order by dias_registro_desde_epoch) as flujos_origen         
          ,sum(case when ward_de_analisis is FALSE then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_flujos_origen
          ,array_agg(case when ward_de_analisis is FALSE AND  linea in ('mastercard', 'mastercard_physical', 'crossborder', 'utility_payments', 'top_ups', 'p2p', 'p2p_received','paypal', 'cash_in_savings', 'cash_out_savings','withdrawal_tyba', 'investment_tyba', 'aum_tyba') then linea else '__WARD__' end) over (partition by user order by dias_registro_desde_epoch) as productos_origen 
          ,array_agg(case when ward_de_analisis is FALSE AND  linea_simplif in ('mastercard','crossborder', 'utility_payments', 'top_ups', 'p2p','paypal', 'bolsillo','tyba') then linea_simplif else '__WARD__' end) over (partition by user order by dias_registro_desde_epoch) as productos_simplif_origen         
          ,sum(case when ward_de_analisis is FALSE  AND  linea in ('mastercard', 'mastercard_physical', 'crossborder','utility_payments', 'top_ups', 'p2p', 'p2p_received','paypal', 'cash_in_savings', 'cash_out_savings','withdrawal_tyba', 'investment_tyba') then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen
          ,sum(case when ward_de_analisis is FALSE  AND linea = 'cash_in' then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_ci
          ,sum(case when ward_de_analisis is FALSE  AND linea = 'cash_out' then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_co
          ,sum(case when ward_de_analisis is FALSE  AND linea in ( 'mastercard', 'mastercard_physical') then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_mc
          ,sum(case when ward_de_analisis is FALSE  AND linea = 'utility_payments' then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_up
          ,sum(case when ward_de_analisis is FALSE  AND linea = 'top_ups' then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_tu
          ,sum(case when ward_de_analisis is FALSE  AND linea IN  ('p2p','p2p_received' ) then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_p2p
          ,sum(case when ward_de_analisis is FALSE  AND linea = 'paypal' then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_pp
          ,sum(case when ward_de_analisis is FALSE  AND linea = 'crossborder' then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_cb
          ,sum(case when ward_de_analisis is FALSE  AND linea IN ( 'aum_savings', 'cash_in_savings','cash_out_savings') then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_bs
          ,sum(case when ward_de_analisis is FALSE  AND linea IN ( 'withdrawal_tyba', 'investment_tyba', 'aum_tyba') then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen_ty      
        FROM economics
        WHERE
            linea in ('mastercard','mastercard_physical', 'crossborder' , 'utility_payments', 'top_ups', 'p2p', 'p2p_received','paypal', 'cash_in_savings', 'cash_out_savings','aum_savings', 'cash_in', 'cash_out','withdrawal_tyba', 'investment_tyba', 'aum_tyba') OR linea is null
    ),summary as (
        SELECT
          *
        FROM datos
        WHERE 
           ward_de_analisis = true 
    ),datos_output as (
        SELECT DISTINCT
          DATE(Fecha_Fin_Analisis) Fecha_Fin_Analisis_DT
          ,user
          ,TO_JSON_STRING(`{{project_source_2}}.aux_table.FILTER_WARDS_DISTINCT`(productos_origen)) AS productos_origen
          ,`{{project_source_2}}.aux_table.DISTINCT_COUNT`(productos_origen) AS uniq_productos_origen
          ,TO_JSON_STRING(`{{project_source_2}}.aux_table.FILTER_WARDS_DISTINCT`(productos_simplif_origen)) AS productos_simplif_origen
          ,`{{project_source_2}}.aux_table.DISTINCT_COUNT`(productos_simplif_origen) AS uniq_productos_simplif_origen
          ,TO_JSON_STRING(`{{project_source_2}}.aux_table.FILTER_WARDS_DISTINCT`(flujos_origen)) flujos_origen
          ,`{{project_source_2}}.aux_table.DISTINCT_COUNT`(flujos_origen) AS uniq_flujos_origen
          ,cuenta_trx_flujos_origen
          ,cuenta_trx_origen
          ,cuenta_trx_origen_ci
          ,cuenta_trx_origen_co
          ,cuenta_trx_origen_mc
          ,cuenta_trx_origen_up
          ,cuenta_trx_origen_tu
          ,cuenta_trx_origen_p2p
          ,cuenta_trx_origen_pp
          ,cuenta_trx_origen_cb
          ,cuenta_trx_origen_bs
          ,cuenta_trx_origen_ty          
        FROM summary
    ),agrupacion_datos as (   
        SELECT 
          *,
          CASE  
          WHEN (cuenta_trx_origen_ci >0 AND cuenta_trx_origen_co > 0 AND cuenta_trx_origen_up + cuenta_trx_origen_tu + cuenta_trx_origen_pp + cuenta_trx_origen_mc + cuenta_trx_origen_p2p + cuenta_trx_origen_bs + cuenta_trx_origen_cb  = 0) THEN 'cashin y cashout'
          WHEN cuenta_trx_origen_bs > 0 THEN 'bolsillo'
          WHEN cuenta_trx_origen_ty > 0 THEN 'ty'
          WHEN cuenta_trx_origen_mc > 0 AND (cuenta_trx_origen_up + cuenta_trx_origen_tu + cuenta_trx_origen_pp > 0 ) THEN  'tarjeta + flujo nndd'
          WHEN cuenta_trx_origen_up + cuenta_trx_origen_tu + cuenta_trx_origen_pp > 0 THEN 'flujo nndd'
          WHEN cuenta_trx_origen_mc > 0 AND (cuenta_trx_origen_p2p + cuenta_trx_origen_cb > 0  OR (cuenta_trx_origen_up + cuenta_trx_origen_tu + cuenta_trx_origen_pp + cuenta_trx_origen_p2p + cuenta_trx_origen_bs = 0)) THEN  'tarjeta + otro flujo'
          ELSE 'otro flujo' END agrupacion
        FROM datos_output 
        )   
   
   SELECT 
    * 
   FROM agrupacion_datos 
   WHERE 
    uniq_flujos_origen > 0




);
