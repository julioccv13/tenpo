{{ config(materialized='table',  tags=["hourly", "bi"]) }}

select *, IFNULL(LAG(total_activos) OVER (PARTITION BY servicio order by servicio,mes) - recurrentes,0) as abandonos, from 
	(
	Select 
	SUM(New_) as nuevos,
	SUM(Recurring) as recurrentes,
	SUM(Returned) as retornos,
	SUM(Returned+Recurring+New_) as total_activos,
	servicio,
  camada,
  mes,
		from (
		select 
		*, 
		IF(month_diff is NULL, 1 , 0) as New_,
		IF(month_diff is NOT NULL AND month_diff<=3, 1 , 0) as Recurring,
		IF(month_diff is NOT NULL AND month_diff>3, 1 , 0) as Returned,
			from
			(
				select *, date_diff(cast(mes as date),cast(last_trx as date),MONTH) as month_diff from
					(
					select *, 
					LAG(mes) OVER (PARTITION BY cli,servicio order by cli,mes,camada) as last_trx,
					FROM 
					(
            SELECT
              FORMAT_DATE('%Y-%m-01', DATE(trx.fec_fechahora_envio)) mes,
              SUM(trx.mto_monto_dolar) as gpv_usd,
              COUNT(1) as trx,
              trx.id_cuenta as cli,
              IF (trx.tip_trx = "ABONO_PAYPAL", "Abonos", "Retiros") as servicio,
              FORMAT_DATE('%Y-%m-01', DATE(cli.fec_hora_ingreso)) as camada,
            FROM {{source('paypal','pay_transaccion')}} as trx
              INNER JOIN {{source('paypal','pay_cuenta')}}  as cue ON trx.id_cuenta = cue.id 
              INNER JOIN {{source('paypal','pay_cliente')}} as cli ON cue.id_cliente = cli.id
            WHERE trx.est_estado_trx IN (2,3,8,17,24)
              AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL')
            GROUP BY mes, cli, servicio, camada
					)
				)
			)
		)
	GROUP BY mes, servicio, camada
	ORDER BY servicio, camada, mes ASC
	)
