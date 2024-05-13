{{ config(materialized='table') }}
with datos as 
(
  with impactados as 
  (
    select distinct 
      t_rut.id_cuenta,
      impactados.fecha_envio,
      impactados.nombre_campana
    from 
      {{source('aux_paypal','aux_funnel_retiros')}} as impactados 
    join
      {{ref('rut_correo')}} as t_rut 
      on impactados.correo=t_rut.correo_cuenta
  )
  SELECT DISTINCT 
  fecha_envio,
  nombre_campana,
  id_cuenta,
  tip_trx,
  val_comision_multicaja,
  valor_dolar_cierre,
  valor_dolar_multicaja,
  id_trx,
  mto_monto_dolar,
  margen,
  CASE
      WHEN fecha_trx IS NOT NULL THEN fecha_trx
      ELSE CAST(fecha_envio AS TIMESTAMP)
      END AS fecha_trx,
  CASE 
      WHEN est_estado_trx in (2,3,8,17,24) then "Exitosa"
      WHEN est_estado_trx is not null then "No Extitosa"
      ELSE "Sin Transacción"
      END as estado,
  FROM
    ( 
      select 
          tip_trx,
          t2.est_estado_trx,
          val_comision_multicaja, 
          valor_dolar_cierre,
          valor_dolar_multicaja,
          fec_fechahora_envio AS fecha_trx,
          EXTRACT(MONTH from fec_fechahora_envio) as month_trx,
          EXTRACT(year from fec_fechahora_envio) as year_trx,
          id as id_trx,
          mto_monto_dolar,
          (CASE 
            WHEN tip_trx = 'ABONO_PAYPAL' THEN (
              val_comision_multicaja/1.19 #Comision sin IVA
              +(valor_dolar_multicaja-valor_dolar_cierre)*mto_monto_dolar #Spread
              -mto_monto_dolar*0.024*valor_dolar_multicaja) #Comision PayPal
            WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
              (val_comision_multicaja/1.19*valor_dolar_multicaja #Comision sin IVA
              +(valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar)) #Spread
            WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
              (val_comision_multicaja/1.19*valor_dolar_multicaja)) #Comision sin IVA
            END)/valor_dolar_multicaja #se pasa a dólares
          as margen,
          t1.id_cuenta,
          t1.nombre_campana,
          t1.fecha_envio,
        from 
          impactados as t1 
        left join
          {{source('paypal','pay_transaccion')}} as t2 on (t1.id_cuenta=t2.id_cuenta)
        where 
          (CAST(fec_fechahora_envio as DATE) BETWEEN fecha_envio AND DATE_ADD(fecha_envio, INTERVAL 10 DAY)) 
          and tip_trx IN ('RETIRO_PAYPAL','RETIRO_APP_PAYPAL')
      ) 
      FULL JOIN 
        impactados USING (fecha_envio,nombre_campana,id_cuenta)
)

select distinct * from datos 
