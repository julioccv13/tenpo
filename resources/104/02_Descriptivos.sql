DROP TABLE IF EXISTS `${project_target}.temp.P_{{ds_nodash}}_104_Temp_Data`;
CREATE TABLE `${project_target}.temp.P_{{ds_nodash}}_104_Temp_Data` AS (
WITH users AS (
        SELECT DISTINCT
            b.rut user,
            correo_cuenta,
            Fecha_Inicio_Analisis,
            Fecha_Fin_Analisis,
            Fecha_Fin_Analisis_DT,
            row_number() over (partition by b.rut order by fec_fecha_hora_act desc) as row_num_actualizacion,
        FROM `${project_source_2}.paypal_2020.pay_cuenta` a
                JOIN `${project_source_2}.paypal_2020.pay_cliente` b ON (a.id_cliente=b.id)
                LEFT JOIN `${project_target}.temp.P_{{ds_nodash}}_104_Params` c ON 1=1 
        WHERE 
            1=1
            and DATE(a.fec_fecha_hora_ingreso) <= Fecha_Fin_Analisis_DT
    ), users_distinct as (
        SELECT 
            * EXCEPT (row_num_actualizacion)
        FROM users
        WHERE row_num_actualizacion = 1
    ), transacciones AS (
        SELECT
            DATE(fecha_trx) as fecha
            ,CAST(fecha_trx AS DATETIME) trx_timestamp
            ,CAST(id_trx AS STRING) as trx_id
            ,tip_trx as linea
            ,b.user
            ,CAST(UNIX_SECONDS(TIMESTAMP(fecha_trx))/(60*60*24) AS INT64) as dias_registro_desde_epoch
            ,a.mto_monto_dolar monto
            ,false as ward_de_analisis
        FROM `${project_source_1}.paypal.transacciones_paypal` a
        JOIN users_distinct b ON b.user = a.rut
        LEFT JOIN `${project_target}.temp.P_{{ds_nodash}}_104_Params` c ON 1=1
        WHERE 
            a.fecha_trx <= c.Fecha_Fin_Analisis_DT
        UNION ALL
        SELECT
        distinct
            c.Fecha_Fin_Analisis
            ,c.Fecha_Fin_Analisis_DT
            ,CAST(null as STRING)
            ,CAST(null as STRING)
            ,user
            ,CAST(UNIX_SECONDS(cast(c.Fecha_Fin_Analisis_DT as timestamp))/(60*60*24) AS INT64) as dias_registro_desde_epoch
            ,null as monto
            ,true as ward_de_analisis
        FROM `${project_source_1}.paypal.transacciones_paypal` a
        JOIN users_distinct b ON b.user = a.rut
        LEFT JOIN `${project_target}.temp.P_{{ds_nodash}}_104_Params` c ON 1=1
        WHERE 
            a.fecha_trx <= c.Fecha_Fin_Analisis_DT
    )
, datos_rfmp as (
        SELECT 
            user
            ,trx_timestamp
            ,linea
            ,monto
            ,dias_registro_desde_epoch
            ,ward_de_analisis
            ,array_agg(case when ward_de_analisis is FALSE then linea else '__WARD__' end) over (partition by user order by dias_registro_desde_epoch range between 360 preceding and current row) as productos_ult360dias
            ,array_agg(case when ward_de_analisis is FALSE then linea else '__WARD__' end) over (partition by user order by dias_registro_desde_epoch) as productos_origen
            ,sum(case when ward_de_analisis is FALSE then monto else null end) over (partition by user order by dias_registro_desde_epoch range between 360 preceding and current row)  as monto_gastado_ultm360d
            ,sum(case when ward_de_analisis is FALSE then monto else null end) over (partition by user order by dias_registro_desde_epoch)  as monto_gastado_origen
            ,sum(case when ward_de_analisis is FALSE then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch range between 360 preceding and current row)  as cuenta_trx_ultm_360d
            ,sum(case when ward_de_analisis is FALSE then 1 else 0 end) over (partition by user order by dias_registro_desde_epoch)  as cuenta_trx_origen
            FROM transacciones
    ), summary as (
        SELECT
          *
        FROM datos_rfmp
        WHERE 
           ward_de_analisis = true 
   ),datos_rfmp_output as (
        SELECT DISTINCT
          user
          ,TO_JSON_STRING(`${project_target}.aux_table.FILTER_WARDS`(productos_ult360dias)) AS productos_ult360dias
          ,`${project_target}.aux_table.DISTINCT_COUNT`(productos_ult360dias) AS uniq_productos_ult360dias
          ,TO_JSON_STRING(`${project_target}.aux_table.FILTER_WARDS`(productos_origen)) AS productos_origen
          ,`${project_target}.aux_table.DISTINCT_COUNT`(productos_origen) AS uniq_productos_origen
          ,monto_gastado_ultm360d
          ,monto_gastado_origen
          ,cuenta_trx_ultm_360d
          ,cuenta_trx_origen
        FROM summary
    ), totals as (
        SELECT
            user,
            MIN(DATE_DIFF(DATE(Fecha_Fin_Analisis) ,DATE(trx_timestamp),DAY)) as recency,
            MIN(DATE_DIFF(DATE(Fecha_Fin_Analisis) ,IF(trx_timestamp>=Fecha_Inicio_Analisis,DATE(trx_timestamp),null),DAY)) as recency_ult360d,
        FROM transacciones a
        JOIN users_distinct b using (user)
        WHERE not ward_de_analisis
        GROUP BY user
    ), datos as (
        SELECT
            a.* 
            ,recency
            ,IF (recency_ult360d>360,null,recency_ult360d) as recency_ult360d
            ,cuenta_trx_origen
            ,IF (cuenta_trx_ultm_360d=0,null,cuenta_trx_ultm_360d) as cuenta_trx_ultm_360d
            ,monto_gastado_origen
            ,IF (monto_gastado_ultm360d=0,null,monto_gastado_ultm360d) as monto_gastado_ultm360d 
            ,uniq_productos_origen
            ,IF (uniq_productos_ult360dias =0,null,uniq_productos_ult360dias ) as uniq_productos_ult360dias  
        FROM users_distinct a
        JOIN totals tot USING (user)
        JOIN datos_rfmp_output USING(user)
     )
     select * from datos
);

