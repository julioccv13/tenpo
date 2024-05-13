{{ config(materialized='table',  tags=["daily", "bi"]) }}

select *, IFNULL(LAG(total_activos) OVER (PARTITION BY categoria,operador order by categoria,operador,mes) - recurrentes,0) as abandonos, from 
	(
	Select 
	#count(1 over status),
	SUM(New_) as nuevos,
	SUM(Recurring) as recurrentes,
	SUM(Returned) as retornos,
	SUM(Returned+Recurring+New_) as total_activos,
	categoria,
  operador,
	mes
		from (
		select 
		*, 
		#IF(month_diff is NULL, "New", IF (month_diff<=2, "Recurring", "Returned")) as status,
		IF(month_diff is NULL, 1 , 0) as New_,
		IF(month_diff is NOT NULL AND month_diff<=2, 1 , 0) as Recurring,
		IF(month_diff is NOT NULL AND month_diff>2, 1 , 0) as Returned,
		#IF (month_diff<=2, "active", "churn") as status
			from
			(
				select *, date_diff(cast(mes as date),cast(last_topup as date),MONTH) as month_diff from
					(
					select *, 
					LAG(mes) OVER (PARTITION BY cli,categoria,operador order by cli,mes) as last_topup,
					from 
					(
					SELECT
					FORMAT_DATE('%Y-%m-01', DATE(r.fecha_recarga)) mes,
					SUM(t.monto) as gpv_clp,
					COUNT(1) as trx,
					#       COUNT(DISTINCT IF(t.id_usuario  > 0, CAST (t.id_usuario AS STRING), (IF(t.id_usuario < 0 AND t.correo_usuario <> "",t.correo_usuario,r.suscriptor)))) AS usr,
					#       MD5(IF(t.id_usuario  > 0, CAST (t.id_usuario AS STRING), (IF(t.id_usuario < 0 AND t.correo_usuario <> "",t.correo_usuario,r.suscriptor)))) as cli,
					MD5(r.suscriptor) as cli,
					#         CASE 
					#          WHEN t.id_usuario  > 0 THEN 'Registrado'
					#          WHEN t.id_usuario < 0 AND t.correo_usuario <> "" THEN 'Semiregistrado'
					#          ELSE 'Anónimo' 
					#          END
					#          AS tipo_usuario,
					tp.nombre as categoria,
          o.nombre as operador,
					FROM {{source("topups_web","ref_transaccion")}}  t
					INNER JOIN {{source("topups_web","ref_recarga")}} r ON t.id = r.id_transaccion
					INNER JOIN {{source('topups_web','ref_producto')}} p ON p.id = r.id_producto 
					INNER JOIN {{source('topups_web','ref_operador')}} o ON o.id = p.id_operador
					INNER JOIN {{source('topups_web','ref_comisiones')}} c ON c.id_producto = p.id 
					INNER JOIN {{source('topups_web','ref_tipo_producto')}} tp ON tp.id = p.id_tipo_producto 
					WHERE t.id_estado = 20 AND r.id_estado = 27 AND t.id_origen IN (1,2,5)
					GROUP BY mes, cli, categoria, operador
					)
				)
			)
		)
	GROUP BY mes, categoria, operador
	ORDER BY categoria, operador, mes ASC
	)
