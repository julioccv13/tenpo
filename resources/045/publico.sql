DROP TABLE IF EXISTS `${project_target}.temp.notifications_history_publico_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.notifications_history_publico_{{ds_nodash}}` AS (

      SELECT 
            *
      FROM `${project_source_1}.prepago.notifications_history` --${project_source_1} => pfraudes
      WHERE  
      fecha >= date_sub('{{ds}}', INTERVAL 1 day)
      AND tipo_trx IN ("1","11","55","2")
      
);

DROP TABLE IF EXISTS `${project_target}.temp.notifications_history_mastercard_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.notifications_history_mastercard_{{ds_nodash}}` AS (

      SELECT 
            d.id,
            d.fecha_creacion,
            d.fecha,
            d.hora_creacion,
            d.fecha_actualizacion,
            d.user,
            d.rut,
            cast(d.contrato as int64) as contrato,
            cast(d.pan as string) as pan,
            d.id_tx_externo,
            cast(d.tipo_trx as integer) tipo_trx,
            d.description_tipo_trx,
            d.estado_trx,
            d.description_estado_trx,
            d.saldo_disponible,
            d.monto_trx,
            d.merchant_code,
            d.merchant_name,
            d.country,
            d.place,
            d.es_comercio_presencial,
            CAST(d.country_iso_code AS int64) country_iso_code,
            d.origen_trx,
            d.adquirente,
            d.decode_b64,
            abs(prepago.total_currency_value) monto_final,
            prepago.codact as mcc,
            substring(d.decode_b64, 200 ,1) as cliente_presente_terminal,
            substring(d.decode_b64, 201 ,1) as tarjeta_presente, 
            substring(d.decode_b64, 202 ,1) as entrada_pos, 
            substring(d.decode_b64, 203 ,1) as autenticacion,
            ipm.PDS0052 AS ucaf,
            prepago.tipo_tarjeta,
            prepago.uuid as uuid_trx
      FROM `${project_source_1}.temp.notifications_history_publico_{{ds_nodash}}` d -- ${project_source_1} => pfraudes
      
      LEFT JOIN (

          SELECT 
                  CAST(RIGHT(CAST(c.cuenta as STRING), 7) as INT64) contrato,
                  p.*,
                  h.total_currency_value,
                  t.tipo as tipo_tarjeta
          FROM `${project_source_2}.prepago.prp_movimiento` p   --${project_source_2} = tenpo-airflow-prod
          JOIN `${project_source_2}.prepago.prp_tarjeta` t ON p.id_tarjeta = t.id
          JOIN `${project_source_2}.prepago.prp_cuenta` c ON c.id = t.id_cuenta
          LEFT JOIN `${project_source_2}.transactions_history.transactions_history` h on h.transaction_id = p.uuid 
          
      )  prepago on cast(prepago.contrato as string) = cast(cast(d.contrato as int64) as string) and prepago.id_tx_externo = d.id_tx_externo

      LEFT JOIN (
        
          SELECT DE2,
                 DE38,
                 DE26,
                 DE6,
                 PDS0052 FROM `${project_source_3}.IPM.IPM`  --${project_source_3} = tenpo-operaciones
          QUALIFY ROW_NUMBER() OVER (PARTITION BY DE2,DE38,DE26,DE6 order by FECHA_CARGA DESC) = 1
          
      ) ipm ON RIGHT(d.pan,4) = RIGHT(ipm.DE2,4) AND d.id_tx_externo = ipm.DE38 AND CAST(prepago.codact AS STRING) = ipm.DE26 AND CAST(d.monto_trx AS STRING) = ipm.DE6    
      QUALIFY ROW_NUMBER() OVER (PARTITION BY id, user ORDER BY fecha_actualizacion DESC) = 1

);


-- Tabla de resultados
CREATE TABLE IF NOT EXISTS `${project_target}.prepago.notifications_history_mastercard`
PARTITION BY fecha
CLUSTER BY user
AS (
    SELECT *
    FROM `${project_target}.temp.notifications_history_mastercard_{{ds_nodash}}`
    LIMIT 0
);

-- INSERT IDEMPOTENTE
DELETE FROM `${project_target}.prepago.notifications_history_mastercard`
WHERE date(fecha) >= date_sub('{{ds}}', INTERVAL 1 day);

INSERT INTO `${project_target}.prepago.notifications_history_mastercard`
SELECT 
      id,
      fecha_creacion,
      fecha,
      hora_creacion,
      fecha_actualizacion,
      user,
      rut,
      contrato,
      pan,
      id_tx_externo,
      cast(tipo_trx as int64) as tipo_trx,
      description_tipo_trx,
      estado_trx,
      description_estado_trx,
      saldo_disponible,
      monto_trx,
      merchant_code,
      merchant_name,
      country,
      place,
      es_comercio_presencial,
      cast(country_iso_code as int64) country_iso_code,
      origen_trx,
      adquirente,
      decode_b64,
      monto_final,
      mcc,
      cliente_presente_terminal,
      tarjeta_presente, 
      entrada_pos, 
      autenticacion,
      ucaf,
      tipo_tarjeta,
      uuid_trx
FROM  `${project_target}.temp.notifications_history_mastercard_{{ds_nodash}}`;