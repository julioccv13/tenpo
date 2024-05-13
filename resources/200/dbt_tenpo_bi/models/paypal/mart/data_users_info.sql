{{ 
  config(
    tags=["hourly", "paypal","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT15', 'tenpo-datalake-sandbox')
  ) 
}}
with data as (
    WITH
      transacciones AS (
      ---Tabla de transacciones
      SELECT
        tip_trx,
        fec_fechahora_envio AS fecha_trx,
        id,
        mto_monto_dolar,
        id_cuenta,
        cor_mail_envio
      FROM
        {{source('paypal','pay_transaccion')}}
      WHERE
        est_estado_trx IN (2,3,8,17,24)
        AND tip_trx IN ('ABONO_PAYPAL','RETIRO_PAYPAL','RETIRO_USD_PAYPAL','RETIRO_APP_PAYPAL')),
      cuentas AS (
      ---Tabla de cuentas
      SELECT
        id AS id_cuenta,
        id_cliente,
        correo_cuenta,
        fec_fecha_hora_ingreso AS fecha_ingreso,
        tip_operacion_multicaja,
      FROM
        {{source('paypal','pay_cuenta')}}
        ),
      clientes AS (
      ---Tabla de clientes
      SELECT
        rut,
        id AS id_cliente,
        correo_usuario,
        tipo_usuario
      FROM
        {{source('paypal','pay_cliente')}}),
      ---Tabla de clientes Tenpo
      clientes_tenpo AS (
         SELECT DISTINCT
         tributary_identifier as rut,
         id as user_id,
         state AS state,
         created_at AS ts_creacion,
         ob_completed_at AS ob_completed_at,
         FROM
         {{source('tenpo_users','users')}} 
         where true
         qualify row_number() over (partition by tributary_identifier order by created_at) = 1

      ),
      categorias as (
        select distinct
            id_cuenta,
            tip_trx,
            case
              when MAX(total_last_12m)>=20000 and tip_trx like "RETIRO%" then "B2B"
              when MAX(total_last_12m)<20000 and tip_trx like "RETIRO%" then "B2C"
              when MAX(total_last_12m)>=10000 and tip_trx like "ABONO%" then "B2B"
              when MAX(total_last_12m)<10000 and tip_trx like "ABONO%" then "B2C"
              end
            as categoria,
        from {{ref('transacciones_paypal')}} 
        where id_cuenta is not null
        GROUP BY id_cuenta, tip_trx
      )
    ---Join de las tablas anteriores
    SELECT
      *,
      LAG(fecha_trx) OVER (PARTITION BY id_cuenta ORDER BY tip_trx, fecha_trx ) AS preceding_trx,
      TIMESTAMP_DIFF(fecha_trx,(LAG(fecha_trx) OVER (PARTITION BY id_cuenta ORDER BY tip_trx, fecha_trx )),DAY) AS tbt,
      min(fecha_trx) OVER (PARTITION BY id_cuenta,tip_trx) AS first_trx,
      max(fecha_trx) OVER (PARTITION BY id_cuenta,tip_trx) AS last_trx,
    FROM
      transacciones
    FULL JOIN (
      SELECT
        *
      FROM
        cuentas
      LEFT JOIN
        clientes
      USING
        (id_cliente)
    )
    USING
      (id_cuenta)
      LEFT JOIN clientes_tenpo USING (rut)
      LEFT JOIN categorias USING (id_cuenta,tip_trx)
  
)
select distinct 
  * except (fecha_trx,id,mto_monto_dolar,preceding_trx,tbt),
  IF(id_cuenta is null, null, SUM(mto_monto_dolar) OVER (PARTITION BY id_cuenta)) AS total_ammount,
  IF(id_cuenta is null, null, AVG(tbt) OVER (PARTITION BY id_cuenta)) AS avg_tbt,
  IF(id_cuenta is null, null, AVG(mto_monto_dolar) OVER (PARTITION BY id_cuenta)) AS avg_ammount,
  IF(id_cuenta is null, null, COUNT(id) OVER (PARTITION BY id_cuenta)) AS total_trx,
from data
where true
qualify row_number() over (partition by id_cuenta ORDER BY fecha_trx) = 1