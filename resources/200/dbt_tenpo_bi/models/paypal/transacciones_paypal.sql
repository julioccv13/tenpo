{{ config(materialized='table',  tags=["hourly", "bi"]) }}

WITH
  datos AS(
    WITH
      transacciones AS (
      ---Tabla de transacciones
      SELECT DISTINCT
        codigo_mc,
        tip_trx,
        val_comision_multicaja, 
        valor_dolar_cierre,
        valor_dolar_multicaja,
        DATETIME(fec_fechahora_envio,"UTC") AS fecha_trx,
        EXTRACT(MONTH from DATETIME(fec_fechahora_envio,"UTC")) as month_trx,
        EXTRACT(year from DATETIME(fec_fechahora_envio,"UTC")) as year_trx,
        id as id_trx,
        mto_monto_dolar,
        id_cuenta,
        (CASE 
          WHEN tip_trx = 'ABONO_PAYPAL' THEN (
            val_comision_multicaja/1.19 #Comision sin IVA
            ) #Comision PayPal
          WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja) #Comision sin IVA
            ) #Spread
          WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja)) #Comision sin IVA
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja)) #Comision sin IVA
          END)/valor_dolar_multicaja #se pasa a dólares
        as ingreso_comision,
        (CASE 
          WHEN tip_trx = 'ABONO_PAYPAL' THEN (
            (valor_dolar_multicaja-valor_dolar_cierre)*mto_monto_dolar #Spread
            ) #Comision PayPal
          WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
            (valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar) #Spread
          WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
            0) #Comision sin IVA
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            (valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar) #Spread
          END)/valor_dolar_multicaja #se pasa a dólares
        as ingreso_tipo_cambio,
        (CASE 
          WHEN tip_trx = 'ABONO_PAYPAL' THEN (
            mto_monto_dolar*0.024*valor_dolar_multicaja) #Comision PayPal
          WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
            0)
          WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
            0)
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            0)
          END)/valor_dolar_multicaja #se pasa a dólares
        as comision_paypal,
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
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja)+
            (valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar) #Comision sin IVA
          END)/valor_dolar_multicaja #se pasa a dólares
        as margen,
        {{target.schema}}.Multiplier(CAST(DATETIME(fec_fechahora_envio,"UTC") AS DATE)) as multiplier,
        mto_monto_trx
      FROM
        {{source('paypal','pay_transaccion')}}
      WHERE
        est_estado_trx IN (2,3,8,17,24)
        AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL')
      UNION ALL
      SELECT DISTINCT
        codigo_mc,
        tip_trx,
        val_comision_multicaja, 
        valor_dolar_cierre,
        valor_dolar_multicaja,
        DATETIME(fec_fechahora_envio,"UTC") AS fecha_trx,
        EXTRACT(MONTH from DATETIME(fec_fechahora_envio,"UTC")) as month_trx,
        EXTRACT(year from DATETIME(fec_fechahora_envio,"UTC")) as year_trx,
        trx.id as id_trx,
        mto_monto_dolar,
        trx.id_cuenta,
        (CASE 
          WHEN tip_trx = 'ABONO_PAYPAL' THEN (
            val_comision_multicaja/1.19 #Comision sin IVA
            ) #Comision PayPal
          WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja) #Comision sin IVA
            ) #Spread
          WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja)) #Comision sin IVA
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja)) #Comision sin IVA
          END)/valor_dolar_multicaja #se pasa a dólares
        as ingreso_comision,
        (CASE 
          WHEN tip_trx = 'ABONO_PAYPAL' THEN (
            (valor_dolar_multicaja-valor_dolar_cierre)*mto_monto_dolar #Spread
            ) #Comision PayPal
          WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
            (valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar) #Spread
          WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
            0) #Comision sin IVA
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            (valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar) #Spread
          END)/valor_dolar_multicaja #se pasa a dólares
        as ingreso_tipo_cambio,
        (CASE 
          WHEN tip_trx = 'ABONO_PAYPAL' THEN (
            mto_monto_dolar*0.024*valor_dolar_multicaja) #Comision PayPal
          WHEN tip_trx = 'RETIRO_PAYPAL' THEN (
            0)
          WHEN tip_trx = 'RETIRO_USD_PAYPAL' THEN (
            0)
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            0)
          END)/valor_dolar_multicaja #se pasa a dólares
        as comision_paypal,
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
          WHEN tip_trx = 'RETIRO_APP_PAYPAL' THEN (
            (val_comision_multicaja/1.19*valor_dolar_multicaja #Comision sin IVA
            +(valor_dolar_cierre-valor_dolar_multicaja)*mto_monto_dolar)) #Spread
          END)/valor_dolar_multicaja #se pasa a dólares
        as margen,
        {{target.schema}}.Multiplier(CAST(DATETIME(fec_fechahora_envio,"UTC") AS DATE)) as multiplier,
        mto_monto_trx,
      FROM {{source('paypal','pay_transaccion')}} trx
          --join {{ref('economics')}} eco on cast(trx.id as STRING)=eco.trx_id
        WHERE
          est_estado_trx IN (2,3,8,17,24)
          AND tip_trx IN ('RETIRO_APP_PAYPAL','ABONO_APP_PAYPAL')
      ),
        
      categoria_cuenta AS (
        
      ---Tabla de categoria_cuenta
      select DISTINCT
      month_trx,
      year_trx,
      tip_trx,
      id_cuenta,
      SUM(total_month) OVER (PARTITION BY tip_trx,id_cuenta order by year_trx,month_trx ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as total_last_12m,
      SUM(IF(tip_trx="ABONO_PAYPAL",0,total_month)) OVER (PARTITION BY id_cuenta order by year_trx,month_trx ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as total_last_12m_retiro,
      SUM(IF(tip_trx="ABONO_PAYPAL",total_month,0)) OVER (PARTITION BY id_cuenta order by year_trx,month_trx ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as total_last_12m_abono,
      SUM(IF(tip_trx="ABONO_PAYPAL",0,total_trx_month)) OVER (PARTITION BY id_cuenta order by year_trx,month_trx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_trx_hist_retiro,
      SUM(IF(tip_trx="ABONO_PAYPAL",total_trx_month,0)) OVER (PARTITION BY id_cuenta order by year_trx,month_trx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_trx_hist_abono,
      SUM(total_trx_month) OVER (PARTITION BY id_cuenta order by year_trx,month_trx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as total_trx_hist,
      from (
        SELECT
        distinct 
          tip as tip_trx,
          month_trx,
          year_trx,
          id_cuent as id_cuenta,
          SUM(mto_monto_dolar) as total_month,
          COUNT(distinct id) as total_trx_month
        FROM
          (select distinct * from 
            (
              (
                select 
                  * 
                from {{source('paypal','pay_transaccion')}}
                where est_estado_trx IN (2,3,8,17,24)
                AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL','RETIRO_APP_PAYPAL')
              ) as trx 
              FULL JOIN (
                select 
                  id_cuenta as id_cuent,
                  tip_trx as tip,
                  month_trx,
                  year_trx
                from {{source('paypal','pay_transaccion')}}
                CROSS JOIN (
                  SELECT 
                    EXTRACT(MONTH from CAST(FORMAT_DATE('%Y-%m-%d', mes) as DATE)) month_trx,
                    EXTRACT(YEAR from CAST(FORMAT_DATE('%Y-%m-%d', mes) as DATE)) year_trx
                  FROM UNNEST(GENERATE_DATE_ARRAY((select CAST(MIN(fec_fechahora_envio) AS DATE) from {{source('paypal','pay_transaccion')}}), CURRENT_DATE(), INTERVAL 1 MONTH)) mes
                )
                where est_estado_trx IN (2,3,8,17,24)
                  AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL','RETIRO_APP_PAYPAL','ABONO_APP_PAYPAL')
                ) as idxmes
              ON (trx.id_cuenta= idxmes.id_cuent AND 
                  trx.tip_trx= idxmes.tip AND 
                  EXTRACT(MONTH from DATETIME(fec_fechahora_envio,"UTC")) = idxmes.month_trx AND 
                  EXTRACT(YEAR from DATETIME(fec_fechahora_envio,"UTC")) = idxmes.year_trx)
            )
          )
        GROUP BY 1,2,3,4
        ORDER BY 1,3,2,4,5
        )
      ORDER BY tip_trx,id_cuenta,year_trx,month_trx
      
      ),
      cuentas AS (
      ---Tabla de cuentas
      SELECT
        distinct
        last_value(id) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as id_cuenta,
        last_value(id_cliente) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as id_cliente,
        last_value(correo_cuenta) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as correo_cuenta,
        last_value(tip_documento_sii) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as tip_documento_sii,
        last_value(est_cuenta_paypal) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as est_cuenta_paypal,
        case 
          when last_value(tip_cuenta_banco_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)='0' then 'VISTA'
          when last_value(tip_cuenta_banco_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)='1' then 'AHORRO'
          when last_value(tip_cuenta_banco_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)='2' then 'CORRIENTE'
          when last_value(tip_cuenta_banco_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)='3' then 'CUENTA RUT'          
          end as tip_cuenta_banco_usuario,
        case
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=1 then 'BANCO DE CHILE - EDWARDS'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=12 then 'BANCO ESTADO'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=14 then 'SCOTIABANK (EX DESARROLLO)'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=16 then 'BANCO DE CREDITO E INVERSIONES'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=27 then 'CORPBANCA'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=28 then 'BANCO BICE'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=31 then 'HSBC BANK'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=37 then 'BANCO SANTANDER'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=39 then 'BANCO ITAU CHILE'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=45 then 'THE BANK OF TOKIO MITSUBISHI LTDA'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=49 then 'BANCO SECURITY'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=51 then 'BANCO FALABELLA'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=54 then 'RABOBANK'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=504 then 'BANCO BBVA'
          when last_value(cod_banco_cta_usuario) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following)=672 then 'COOPEUCH'
          end as cod_banco_cta_usuario,
        concat(last_value(tip_operacion_multicaja) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following),"_PAYPAL") as tip_trx,
        last_value(DATETIME(fec_fecha_hora_ingreso,"UTC")) OVER (PARTITION by id order by fec_fecha_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as fecha_ingreso_cuenta,
      FROM
        {{source('paypal','pay_cuenta')}} 
        ),
      clientes AS (
      ---Tabla de clientes
      SELECT 
        distinct
        last_value(id) OVER (PARTITION by id order by fec_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as id_cliente,
        last_value(rut) OVER (PARTITION by id order by fec_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as rut,
        last_value(correo_usuario) OVER (PARTITION by id order by fec_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as correo_usuario,
        last_value(fec_hora_ingreso) OVER (PARTITION by id order by fec_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as fecha_ingreso_cliente,
        last_value(tipo_usuario) OVER (PARTITION by id order by fec_hora_ingreso ROWS BETWEEN unbounded preceding and unbounded following) as tipo_usuario
      FROM
        {{source('paypal','pay_cliente')}}),
    ---Tabla Grandes Cuentas
    grandes_cuentas as (
      with unicas as (
        select DISTINCT
          concat(tipo_operacion,"_PAYPAL") as tip_trx,
          id_cliente,
          DATETIME(fecha_ingreso,"UTC") as fecha_inicio_te,
          DATETIME(IF(fecha_desactivacion<'1900-01-01',NULL,fecha_desactivacion),"UTC") as fecha_fin_te,
          id_estado AS status_te,
          tarifa,
          ROW_NUMBER() OVER (PARTITION BY id_cliente,tipo_operacion order by fecha_actualizacion desc) as row_n,
        from 
          {{source('paypal','pay_gran_empresa')}}
      )
      select * except (row_n) from unicas where row_n=1
      
    )
    ---Join de las tablas anteriores
    SELECT 
    distinct
    *,
    case
      when sum(total_trx_hist) over (partition by id_cliente,month_trx,year_trx) = 1 then 'One Transactor'
      when total_last_12m_retiro >= 24000 AND tip_trx <> 'ABONO_PAYPAL' THEN 'Alto Valor'
      when total_last_12m_retiro >= 2000 AND tip_trx <> 'ABONO_PAYPAL' THEN 'Bajo Valor - A'
      when total_last_12m_retiro < 2000 AND tip_trx <> 'ABONO_PAYPAL' THEN 'Bajo Valor - B'
      when total_last_12m >= 10000 AND tip_trx = 'ABONO_PAYPAL' THEN 'Alto Valor - A'
      when total_last_12m >= 3000 AND tip_trx = 'ABONO_PAYPAL' THEN 'Alto Valor - B'
      when total_last_12m < 3000 AND tip_trx = 'ABONO_PAYPAL' THEN 'Bajo Valor'
      ELSE null 
      END
      as tip_cliente,
    case
      when tipo_usuario = "EMPRESA" 
      or (total_last_12m_retiro>=24000)
      or (total_last_12m_abono>=10000)
        then 'B2B'
      ELSE "B2C" 
      END
      as tipo_mercado,
    IF(sum(total_trx_hist) over (partition by id_cliente,month_trx,year_trx)=1,"One Transactor","Recuerrente") AS tipo_usuario_trx,
    CASE  
      WHEN status_te = 1 AND fecha_trx>=fecha_inicio_te then "Si"
      WHEN status_te = 0 AND fecha_trx>=fecha_inicio_te AND fecha_trx<fecha_fin_te then "Si"
      ELSE "No"
      END as tarifa_espcecial, 
    FROM
    transacciones 
    JOIN categoria_cuenta USING (id_cuenta,month_trx,year_trx,tip_trx)
    FULL JOIN cuentas USING (id_cuenta, tip_trx)
    FULL JOIN clientes USING (id_cliente)
    LEFT JOIN grandes_cuentas using (id_cliente,tip_trx)
  )
SELECT DISTINCT
*,
        IF(
            first_value(extract(month from fecha_trx)) OVER (partition by id_cliente,tip_trx order by fecha_trx ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            =extract(month from fecha_trx)
            AND
            first_value(extract(year from fecha_trx)) OVER (partition by id_cliente,tip_trx order by fecha_trx ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            =extract(year from fecha_trx),
            "Nuevo","Antiguo"
        ) as primera_vez_mes,
IF ( 
  EXTRACT(DAY FROM fecha_trx)<21, 
  concat(year_trx,"-",      
    IF(
      month_trx<10,
      concat("0",month_trx),
      CAST(month_trx AS STRING)
    )
  ), 
  IF(
    month_trx=12,
    concat(year_trx+1,"-01"),
    concat(year_trx,"-",
      IF(
        month_trx<9,
        concat("0",month_trx+1),
        CAST(month_trx+1 AS STRING)
        )
    ))) as mes_map
FROM datos