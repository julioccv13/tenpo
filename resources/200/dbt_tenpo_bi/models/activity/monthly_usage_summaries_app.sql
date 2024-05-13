{{ config(tags=["daily", "bi"], materialized='table') }}

SELECT
 Fecha_Fin_Analisis_DT
 ,activo_mes_app
 ,count(distinct tenpo_uuid) cuenta
 ,AVG(IF (recencia_act_general is not null, recencia_act_general , null)) promedio_recencia_act_general
 ,AVG(IF (recencia_act_mc is not null, recencia_act_mc , null)) promedio_recencia_act_mc
 ,AVG(IF (recencia_act_tu is not null, recencia_act_tu , null)) promedio_recencia_act_tu
 ,AVG(IF (recencia_act_up is not null, recencia_act_up , null)) promedio_recencia_act_up
 ,AVG(IF (recencia_act_cashin is not null, recencia_act_cashin , null)) promedio_recencia_act_cashin
 ,AVG(IF (recencia_act_cashout is not null, recencia_act_cashout , null)) promedio_recencia_act_cashout
 ,AVG(IF (recencia_bolsillo_act_cashin is not null, recencia_act_cashin , null)) promedio_recencia_bolsillo_act_cashin
 ,AVG(IF (recencia_bolsillo_act_cashout is not null, recencia_act_cashout , null)) promedio_recencia_bolsillo_act_cashout
 ,AVG(IF (recencia_p2p is not null, recencia_p2p , null)) promedio_recencia_p2p
 ,AVG(IF (recencia_act_pp_retiro_app is not null, recencia_act_pp_retiro_app , null)) promedio_recencia_act_pp_retiro_app
 ,AVG(IF (avg_deltat_act_general_origen is not null, avg_deltat_act_general_origen/24, null)) prom_avg_deltat_act_general_origen
 ,AVG(IF (avg_deltat_mc_origen is not null, avg_deltat_mc_origen/24, null)) prom_avg_deltat_mc_origen
 ,AVG(IF (avg_deltat_tu_origen is not null, avg_deltat_tu_origen/24, null)) prom_avg_deltat_tu_origen
 ,AVG(IF (avg_deltat_up_origen is not null, avg_deltat_up_origen/24, null)) prom_avg_deltat_up_origen
 ,AVG(IF (avg_deltat_cashin_general_origen is not null, avg_deltat_cashin_general_origen/24, null)) prom_avg_deltat_cashin_general_origen
 ,AVG(IF (avg_deltat_cashout_general_origen is not null, avg_deltat_cashout_general_origen/24, null)) prom_avg_deltat_cashout_general_origen
 ,AVG(IF (avg_deltat_cashin_bolsillo_general_origen is not null, avg_deltat_cashin_bolsillo_general_origen/24, null)) prom_avg_deltat_cashin_bolsillo_general_origen
 ,AVG(IF (avg_deltat_cashout_bolsillo_general_origen is not null, avg_deltat_cashout_bolsillo_general_origen/24, null)) prom_avg_deltat_cashout_bolsillo_general_origen
 ,AVG(IF (avg_deltat_p2p_origen is not null, avg_deltat_p2p_origen/24, null)) prom_avg_deltat_p2p_origen
 ,AVG(IF (avg_deltat_pp_retiro_app_origen  is not null, avg_deltat_pp_retiro_app_origen/24, null)) prom_avg_deltat_pp_retiro_app_origen 
 ,APPROX_QUANTILES( recencia_act_general, 100)[offset(50)] median_recencia_act_general
 ,APPROX_QUANTILES( recencia_act_mc, 100)[offset(50)] median_recencia_act_mc
 ,APPROX_QUANTILES( recencia_act_tu, 100)[offset(50)] median_recencia_act_tu
 ,APPROX_QUANTILES( recencia_act_up, 100)[offset(50)] median_recencia_act_up
 ,APPROX_QUANTILES( recencia_act_cashin, 100)[offset(50)] median_recencia_act_cashin
 ,APPROX_QUANTILES( recencia_act_cashout, 100)[offset(50)] median_recencia_act_cashout
 ,APPROX_QUANTILES( recencia_bolsillo_act_cashin, 100)[offset(50)] median_recencia_bolsillo_act_cashin
 ,APPROX_QUANTILES( recencia_bolsillo_act_cashout, 100)[offset(50)] median_recencia_bolsillo_act_cashout
 ,APPROX_QUANTILES( recencia_p2p, 100)[offset(50)] median_recencia_p2p
 ,APPROX_QUANTILES( recencia_act_pp_retiro_app, 100)[offset(50)] median_recencia_act_pp_retiro_app
 ,APPROX_QUANTILES( avg_deltat_mc_origen/24, 100)[offset(50)] median_avg_deltat_mc_origen
 ,APPROX_QUANTILES( avg_deltat_act_general_origen/24, 100)[offset(50)] median_avg_deltat_act_general_origen
 ,APPROX_QUANTILES( avg_deltat_tu_origen/24, 100)[offset(50)] median_avg_deltat_tu_origen
 ,APPROX_QUANTILES( avg_deltat_up_origen/24, 100)[offset(50)] median_avg_deltat_up_origen
 ,APPROX_QUANTILES( avg_deltat_cashin_general_origen/24, 100)[offset(50)] median_avg_deltat_cashin_general_origen
 ,APPROX_QUANTILES( avg_deltat_cashout_general_origen/24, 100)[offset(50)] median_avg_deltat_cashout_general_origen
 ,APPROX_QUANTILES( avg_deltat_cashin_bolsillo_general_origen/24 , 100)[offset(50)] median_avg_deltat_cashin_bolsillo_general_origen
 ,APPROX_QUANTILES( avg_deltat_cashout_bolsillo_general_origen/24, 100)[offset(50)] median_avg_deltat_cashout_bolsillo_general_origen
 ,APPROX_QUANTILES( avg_deltat_p2p_origen/24, 100)[offset(50)] median_avg_deltat_p2p_origen
 ,APPROX_QUANTILES( avg_deltat_pp_retiro_app_origen/24 , 100)[offset(50)] median_avg_deltat_pp_retiro_app_origen
FROM  {{source('tablones_analisis','tablon_monthly_vectores_usuarios')}}
WHERE 
    tenpo_uuid is not null  
GROUP BY 
    1,2
ORDER BY 
    1 ASC

-- recencia_act_general
-- recencia_act_mc
-- recencia_act_up
-- recencia_act_cashin
-- recencia_act_cashout
-- recencia_p2p
-- recencia_act_pp_retiro_app
-- avg_deltat_mc_origen
-- avg_deltat_act_general_origen
-- avg_deltat_tu_origen
-- avg_deltat_up_origen
-- avg_deltat_cashin_general_origen
-- avg_deltat_cashout_general_origen
-- avg_deltat_p2p_origen
-- avg_deltat_pp_retiro_app_origen 