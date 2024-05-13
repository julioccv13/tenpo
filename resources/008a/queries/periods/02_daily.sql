--- 1 universo de usuarios
CREATE TEMP FUNCTION CLEVERTAP_TIME(UNIX_TIME INT64)
  RETURNS STRING
  AS (
      IF(UNIX_TIME IS NOT NULL, '$D_' || CAST(CAST(UNIX_TIME AS INT64) AS STRING), NULL)
    );


--- cliente ready
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_cliente_ready_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_cliente_ready_{{ds_nodash}}` AS (

    select 
        distinct
            users.email,
            tabl.cliente_ready,
    from  `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` users
    inner join (
        SELECT 
            user 
            ,coalesced_churn = 0 as cliente_ready
        FROM `${project_target}.churn.tablon_daily_eventos_churn` 
        where fecha_fin_analisis_dt = (SELECT MAX(fecha_fin_analisis_dt) FROM `${project_target}.churn.tablon_daily_eventos_churn`)
        and (cierre_cuenta = 0 or cierre_de_cuenta_pasado = 0) and onboarding =1
    ) tabl using (user)

);

--- politica de toques
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_user_touch_policy_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_user_touch_policy_{{ds_nodash}}` AS (

with target as 

    (select
        email,
        case 
        when no_molestar = true and reason = 'open ticket' THEN 'true - high'
        when no_molestar = true and reason = 'exceed touches' THEN 'true - medium'
        when no_molestar = false then 'false'
        END as no_molestar,
        reason,
        close_date_analysis	
    from `${project_source4}.external.today_all_users_touch_policy`)
        
select  email,
        no_molestar
from target

);

--- saldo en la app
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_users_balance_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_users_balance_{{ds_nodash}}` AS (
    
SELECT 
    DISTINCT
    A.user,
    B.email,
    saldo_app as saldo
FROM `${project_source4}.balance.saldo_app` A
JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B ON A.user = B.user
WHERE true
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY fecha DESC) = 1

);


--- saldo en bolsillo
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_users_savings_balance_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_users_savings_balance_{{ds_nodash}}` AS (
select
    distinct
    email,
    round(current_fee_quantity * (select amount from `${project_source1}.payment_savings.fees` order by valid_on_date desc limit 1)) as saldo_bolsillo
from
  `${project_source1}.payment_savings.user_position` savings
join `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` users on users.user = savings.user_id
WHERE true
QUALIFY ROW_NUMBER() OVER (PARTITION BY email ORDER BY calculate_date DESC) = 1

);


--- Otros prepago
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_Cashin_Cashout_Analysis_Adhoc_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_Cashin_Cashout_Analysis_Adhoc_{{ds_nodash}}` AS (
    SELECT
        trx.userId
        ,CASE WHEN type ='CASHIN_TEF' THEN 'cashin' WHEN type = 'CASHOUT_TEF' THEN 'cashout' END AS tipo_trx
        ,'tef_cca' AS canal

        ,CAST(trx.id AS STRING) AS id_trx
        ,CAST(amount AS NUMERIC) AS monto_trx
        ,trx.created AS ts_trx
    
        ,COALESCE(origin_bank.name, '___TENPO_APP___') AS cico_banco_tienda_origen
        ,COALESCE(destination_bank.name, '___TENPO_APP___') AS cico_banco_tienda_destino

        ,originaccountnumber as cuenta_origen
        ,destinationaccountnumber as cuenta_destino

        ,ROW_NUMBER() OVER (PARTITION BY trx.id ORDER BY trx.updated DESC) AS ro_num_actualizacion

    FROM `${project_source1}.payment_cca.payment_transaction` TRX
        LEFT JOIN `${project_source1}.payment_cca.bank` origin_bank 
        ON CAST(trx.originSbifCode AS INT64) = origin_bank.sbif_code
        LEFT JOIN `${project_source1}.payment_cca.bank` destination_bank 
        ON CAST(trx.destinationSbifCode AS INT64) = destination_bank.sbif_code
    WHERE 
        STATUS = 'AUTHORIZED'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trx.id ORDER BY trx.updated DESC) = 1
);

DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_prepagos_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_prepagos_{{ds_nodash}}` AS (
    with data as (
        SELECT 
            distinct
            userId
            ,B.email
            ,MAX(cico_banco_tienda_destino = 'COOPEUCH' OR cico_banco_tienda_origen = 'COOPEUCH') as tiene_coopeuch
            ,MAX(cico_banco_tienda_destino = 'LOS HEROES' OR cico_banco_tienda_origen = 'LOS HEROES') as tiene_los_heroes

            ,MAX((cico_banco_tienda_destino = 'BCI/TBANC/NOVA' AND LEFT(LTRIM(cuenta_destino, '0'), 3) = '777') OR (cico_banco_tienda_origen = 'BCI/TBANC/NOVA' AND LEFT(LTRIM(cuenta_origen, '0'), 3) = '777')) as tiene_mach
            ,MAX((cico_banco_tienda_destino = 'BANCO RIPLEY' AND LEFT(LTRIM(cuenta_destino, '0'), 3) = '999') OR (cico_banco_tienda_origen = 'BANCO RIPLEY' AND LEFT(LTRIM(cuenta_origen, '0'), 3) = '999')) as tiene_check
            ,MAX((cico_banco_tienda_destino = 'BANCO SANTANDER/BANEFE' AND LEFT(LTRIM(cuenta_destino, '0'), 3) = '710') OR (cico_banco_tienda_origen = 'BANCO SANTANDER/BANEFE' AND LEFT(LTRIM(cuenta_origen, '710'), 3) = '710')) as tiene_superdigital

        FROM `${project_target}.temp.clevertap_injection_{{period}}_Cashin_Cashout_Analysis_Adhoc_{{ds_nodash}}` A
        JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.userId = B.user
        GROUP BY 1,2
    )

    select 
    *
    ,cast(tiene_coopeuch as int64) + cast(tiene_los_heroes as int64) + cast(tiene_mach as int64) + cast(tiene_check as int64) + cast(tiene_superdigital as int64) as cuentas_prepago_externas
    from data
);

--- Solicito TF 
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_solicito_tf_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_solicito_tf_{{ds_nodash}}` AS (

    SELECT
        DISTINCT
        B.email,
        true as solicito_tarjeta_fisica
    FROM `${project_source4}.physical_card.orders` A
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.user = B.user
   
);


--- Estado de funnel de TF
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_tf_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_tf_{{ds_nodash}}` AS (

    SELECT
        DISTINCT
        B.email,
        paso estado_solicitud_tarjeta_fisica,
    FROM `${project_source4}.funnel.funnel_physical_card` A
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.user = B.user
    WHERE true
    AND paso <> 'OB exitoso'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY A.user ORDER BY num DESC) = 1


);


--- Estado de funnel Paypal
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_paypal_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_paypal_{{ds_nodash}}` AS (

    SELECT 
      DISTINCT
        B.email,
        paso as paso_funnel_crossborder
    FROM `${project_source4}.funnel.funnel_firebase` A
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.user_id = B.user
    WHERE funnel = 'crossborder'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY paso DESC) = 1


);


--- Comercio en base a recurrencia
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_comercio_recurrente_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_comercio_recurrente_{{ds_nodash}}` AS (

    SELECT 
        DISTINCT
        B.email,
    NOM_Comercio as comercio_recurrente
    FROM `${project_source4}.aux.Comercio_Recurrencia` A 
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.cliente = B.user

);



--- Comercio en base a monto
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_comercio_monto_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_comercio_monto_{{ds_nodash}}` AS (

    SELECT 
        DISTINCT
        B.email,
    NOM_Comercio as comercio_monto
    FROM `${project_source4}.aux.Comercio_Monto` A 
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.cliente = B.user

);


--- Insertar cliente aleatorio (valor por defecto 0 o 1)
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_crm_cliente_aleatorio_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_crm_cliente_aleatorio_{{ds_nodash}}` AS (

    SELECT 
        DISTINCT
        B.email,
        CAST(RAND AS STRING) AS cliente_aleatorio
    FROM `${project_source3}.crm.CLIENTES_ALEATORIO` A 
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on A.ID = B.user

);

--- Vinculados de paypal en la APP
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_paypal_vinculo_app_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_paypal_vinculo_app_{{ds_nodash}}` AS (

    SELECT
        DISTINCT 
            B.email,
            "true" as vinculo_paypal_app
    FROM `${project_source4}.paypal.transacciones_paypal` t 
    JOIN `${project_source4}.users.users_tenpo` u on (t.rut=u.tributary_identifier)
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on u.id = B.user
    WHERE t.tip_trx='RETIRO_APP_PAYPAL'

);

--- Comercio 3M
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_comercio_3m_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_comercio_3m_{{ds_nodash}}` AS (

    SELECT
        DISTINCT 
            B.email,
            A.cliente_comercio_3M
    FROM `${project_source3}.crm.users_comercio_recurrente` A
    JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B on B.user = A.user

);



--- Ultimas transacciones
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_last_fechas_economics`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_last_fechas_economics` AS (
    
    SELECT
        C.email
        ,UNIX_SECONDS(MAX(IF(linea='mastercard' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_compra_mastercard
        ,UNIX_SECONDS(MAX(IF(linea='mastercard_physical' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_compra_mastercard_fisica
        ,UNIX_SECONDS(MAX(IF(linea='crossborder' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_crossborder
        ,UNIX_SECONDS(MAX(IF(linea='top_ups' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_top_ups
        ,UNIX_SECONDS(MAX(IF(linea='utility_payments' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_utility_payments
        ,UNIX_SECONDS(MAX(IF(linea='cash_in' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_cashin
        ,UNIX_SECONDS(MAX(IF(linea='cash_out' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_cashout
        ,UNIX_SECONDS(MAX(IF(linea='p2p' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_p2p_enviado
        ,UNIX_SECONDS(MAX(IF(linea='p2p_received' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_p2p_recibido
        ,UNIX_SECONDS(MAX(IF(linea='paypal' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_paypal
        ,UNIX_SECONDS(MAX(IF(linea='cash_in_savings' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_cashin_savings
        ,UNIX_SECONDS(MAX(IF(linea='cash_in_savings' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_cashout_savings
        ,UNIX_SECONDS(MAX(IF(linea='withdrawal_tyba' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_cashout_tyba
        ,UNIX_SECONDS(MAX(IF(linea='investment_tyba' and nombre not like '%Devol%', trx_timestamp, null))) as ult_fecha_cashin_tyba
    FROM `${project_source4}.economics.economics` A
    INNER JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` B ON A.user = B.user
    INNER JOIN `${project_source1}.users.users` C on A.user = C.id
    group by 1
);

--- ---------------------------------------------  saved history  ---------------------------------------------------------------------

DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_detailed_{{period}}_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_detailed_{{period}}_{{ds_nodash}}` AS (

    SELECT
            users.email as identity
            ,users.fecha_ejec
            ,users.state
            ,CLEVERTAP_TIME(users.date_of_birth)  as date_of_birth
            ,case when c.no_molestar is null then "false" else c.no_molestar end as no_molestar
            ,d.saldo
            ,e.saldo_bolsillo  
            ,CLEVERTAP_TIME(h.ult_fecha_compra_mastercard) as ult_fecha_compra_mastercard
            ,CLEVERTAP_TIME(h.ult_fecha_compra_mastercard_fisica) as ult_fecha_compra_mastercard_fisica
            ,CLEVERTAP_TIME(h.ult_fecha_crossborder) as ult_fecha_crossborder
            ,CLEVERTAP_TIME(h.ult_fecha_top_ups) as ult_fecha_top_ups
            ,CLEVERTAP_TIME(h.ult_fecha_utility_payments) as ult_fecha_utility_payments
            ,CLEVERTAP_TIME(h.ult_fecha_cashin) as ult_fecha_cashin
            ,CLEVERTAP_TIME(h.ult_fecha_cashout) as ult_fecha_cashout
            ,CLEVERTAP_TIME(h.ult_fecha_p2p_enviado) as ult_fecha_p2p_enviado
            ,CLEVERTAP_TIME(h.ult_fecha_p2p_recibido) as ult_fecha_p2p_recibido
            ,CLEVERTAP_TIME(h.ult_fecha_paypal) as ult_fecha_paypal
            ,CLEVERTAP_TIME(h.ult_fecha_cashin_savings) as ult_fecha_cashin_savings
            ,CLEVERTAP_TIME(h.ult_fecha_cashout_savings) as ult_fecha_cashout_savings
            ,CLEVERTAP_TIME(h.ult_fecha_cashout_tyba) as ult_fecha_cashout_tyba
            ,CLEVERTAP_TIME(h.ult_fecha_cashin_tyba) as ult_fecha_cashin_tyba
            ,coalesce(j.cliente_ready, true) as cliente_ready
            ,case when k.solicito_tarjeta_fisica is null then "false" end as solicito_tarjeta_fisica
            ,coalesce(l.estado_solicitud_tarjeta_fisica, NULL) estado_solicitud_tarjeta_fisica
            ,coalesce(m.paso_funnel_crossborder, NULL) paso_funnel_crossborder
            ,coalesce(n.comercio_recurrente, NULL) comercio_recurrente
            ,coalesce(o.comercio_monto, NULL) comercio_monto
            ,coalesce(CAST(p.cliente_aleatorio AS STRING), NULL) cliente_aleatorio
            ,coalesce(CAST(q.vinculo_paypal_app AS STRING), NULL) vinculo_paypal_app
            ,coalesce(CAST(r.cliente_comercio_3M AS STRING), NULL) cliente_comercio_3M

    FROM `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` users
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_user_touch_policy_{{ds_nodash}}` c using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_users_balance_{{ds_nodash}}` d using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_users_savings_balance_{{ds_nodash}}` e using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_last_fechas_economics` h using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_cliente_ready_{{ds_nodash}}` j using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_solicito_tf_{{ds_nodash}}` k using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_tf_{{ds_nodash}}` l using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_paypal_{{ds_nodash}}` m using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_comercio_recurrente_{{ds_nodash}}` n using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_comercio_monto_{{ds_nodash}}` o using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_crm_cliente_aleatorio_{{ds_nodash}}` p using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_paypal_vinculo_app_{{ds_nodash}}` q using (email)
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_comercio_3m_{{ds_nodash}}` r using (email)

);

/*
Cuando se agreguen nuevas properties hay que hacer un ALTER TABLE
    
    Ejemplos:
        ALTER TABLE `${project_target}.aux_table.clevertap_daily_history`
        ADD COLUMN paso_funnel_crossborder STRING,
        ADD COLUMN comercio_recurrente STRING,
        ADD COLUMN comercio_monto STRING,
        ADD COLUMN cliente_aleatorio STRING,
        ADD COLUMN vinculo_paypal_app STRING,
        ADD COLUMN cliente_comercio_3M STRING
*/

CREATE TABLE IF NOT EXISTS `${project_target}.aux_table.clevertap_daily_history`
PARTITION BY date(fecha_ejec)
CLUSTER BY identity
AS (

    SELECT
        *
    FROM `${project_target}.temp.clevertap_injection_detailed_{{period}}_{{ds_nodash}}`
    LIMIT 0

);

/*
    Indempotencia y Borrado (para mantener solo 3 ultimos dias -> esto con el objetivo de poder comparar si los valores cambiar de un dia a otro)
*/

DELETE FROM `${project_target}.aux_table.clevertap_daily_history`
WHERE fecha_ejec = (SELECT MAX(fecha_ejec) FROM `${project_target}.aux_table.clevertap_daily_history`);

-- DELETE FROM `${project_target}.aux_table.clevertap_daily_history`
-- WHERE fecha_ejec < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY);

/*
    Insertar nuevos datos

*/

INSERT INTO `${project_target}.aux_table.clevertap_daily_history`
  SELECT 
        * 
  FROM `${project_target}.temp.clevertap_injection_detailed_{{period}}_{{ds_nodash}}`
;

/*
    GENERAR atributo de "envio payload" -> la idea es enviar el payload solo a los usuarios cuyos atributos cambian
*/

DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_envio_payload_final_{{period}}_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_envio_payload_final_{{period}}_{{ds_nodash}}` AS (

    WITH pre_target AS

    (  select 
            identity,
            fecha_ejec,
            state,
            lead(state) over (partition by identity order by fecha_ejec desc) previous_state,
            date_of_birth,
            no_molestar,
            lead(no_molestar) over (partition by identity order by fecha_ejec desc) previous_no_molestar,
            solicito_tarjeta_fisica,
            lead(solicito_tarjeta_fisica) over (partition by identity order by fecha_ejec desc) previous_solicito_tarjeta_fisica,
            estado_solicitud_tarjeta_fisica,
            lead(estado_solicitud_tarjeta_fisica) over (partition by identity order by fecha_ejec desc) previous_estado_solicitud_tarjeta_fisica,
            saldo,
            lead(saldo) over (partition by identity order by fecha_ejec desc) previous_saldo,
            saldo_bolsillo,
            lead(saldo_bolsillo) over (partition by identity order by fecha_ejec desc) previous_saldo_bolsillo,
            ult_fecha_compra_mastercard,
            lead(ult_fecha_compra_mastercard) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_compra_mastercard,
            ult_fecha_compra_mastercard_fisica,
            lead(ult_fecha_compra_mastercard_fisica) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_compra_mastercard_fisica,
            ult_fecha_crossborder,
            lead(ult_fecha_crossborder) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_crossborder,
            ult_fecha_top_ups,
            lead(ult_fecha_top_ups) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_top_ups,
            ult_fecha_utility_payments,
            lead(ult_fecha_utility_payments) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_utility_payments,
            ult_fecha_cashin,
            lead(ult_fecha_cashin) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_cashin,
            ult_fecha_cashout,
            lead(ult_fecha_cashout) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_cashout,
            ult_fecha_p2p_enviado,
            lead(ult_fecha_p2p_enviado) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_p2p_enviado,
            ult_fecha_p2p_recibido,
            lead(ult_fecha_p2p_recibido) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_p2p_recibido,
            ult_fecha_paypal,
            lead(ult_fecha_paypal) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_paypal,
            ult_fecha_cashin_savings,
            lead(ult_fecha_cashin_savings) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_cashin_savings,
            ult_fecha_cashout_savings,
            lead(ult_fecha_cashout_savings) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_cashout_savings,
            ult_fecha_cashout_tyba,
            lead(ult_fecha_cashout_tyba) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_cashout_tyba,
            ult_fecha_cashin_tyba,
            lead(ult_fecha_cashin_tyba) over (partition by identity order by fecha_ejec desc) previous_ult_fecha_cashin_tyba,
            cliente_ready,
            lead(cliente_ready) over (partition by identity order by fecha_ejec desc) previous_cliente_ready,
            paso_funnel_crossborder,
            lead(paso_funnel_crossborder) over (partition by identity order by fecha_ejec desc) previous_paso_funnel_crossborder,
            comercio_recurrente,
            lead(comercio_recurrente) over (partition by identity order by fecha_ejec desc) previous_comercio_recurrente,
            comercio_monto,
            lead(comercio_monto) over (partition by identity order by fecha_ejec desc) previous_comercio_monto,
            cliente_aleatorio,
            lead(cliente_aleatorio) over (partition by identity order by fecha_ejec desc) previous_cliente_aleatorio,
            vinculo_paypal_app,
            lead(vinculo_paypal_app) over (partition by identity order by fecha_ejec desc) previous_vinculo_paypal_app,
            cliente_comercio_3M,
            lead(cliente_comercio_3M) over (partition by identity order by fecha_ejec desc) previous_cliente_comercio_3M,

        from `${project_target}.aux_table.clevertap_daily_history`
        order by identity desc, fecha_ejec desc

    ), filter_data AS (


      SELECT
         *
        ,case when state != previous_state then true else false end as enviar_payload_state 
        ,case when no_molestar != coalesce(no_molestar,"null") then true else false end as enviar_payload_no_molestar 
        ,case when cast(saldo as string) != coalesce(cast(previous_saldo as string),"null") then true else false end as enviar_payload_saldo
        ,case when cast(saldo_bolsillo as string) != coalesce(cast(previous_saldo_bolsillo as string),"null") then true else false end as enviar_payload_saldo_bolsillo
        ,case when ult_fecha_compra_mastercard != coalesce(previous_ult_fecha_compra_mastercard,"null") then true else false end as enviar_payload_ult_fecha_compra_mastercard
        ,case when ult_fecha_compra_mastercard_fisica != coalesce(previous_ult_fecha_compra_mastercard_fisica,"null") then true else false end as enviar_payload_ult_fecha_compra_mastercard_fisica
        ,case when ult_fecha_crossborder != coalesce(previous_ult_fecha_crossborder,"null") then true else false end as enviar_payload_ult_fecha_crossborder
        ,case when ult_fecha_top_ups != coalesce(previous_ult_fecha_top_ups,"null") then true else false end as enviar_payload_ult_fecha_top_ups
        ,case when ult_fecha_utility_payments != coalesce(previous_ult_fecha_utility_payments,"null") then true else false end as enviar_payload_ult_fecha_utility_payments
        ,case when ult_fecha_cashin != coalesce(previous_ult_fecha_cashin,"null") then true else false end as enviar_payload_ult_fecha_cashin
        ,case when ult_fecha_cashout != coalesce(previous_ult_fecha_cashout,"null") then true else false end as enviar_payload_ult_fecha_cashout
        ,case when ult_fecha_p2p_enviado != coalesce(previous_ult_fecha_p2p_enviado,"null") then true else false end as enviar_payload_ult_fecha_p2p_enviado
        ,case when ult_fecha_p2p_recibido != coalesce(previous_ult_fecha_p2p_recibido,"null") then true else false end as enviar_payload_ult_fecha_p2p_recibido
        ,case when ult_fecha_paypal != coalesce(previous_ult_fecha_paypal,"null") then true else false end as enviar_payload_ult_fecha_paypal
        ,case when ult_fecha_cashin_savings != coalesce(previous_ult_fecha_cashin_savings,"null") then true else false end as enviar_payload_ult_fecha_cashin_savings
        ,case when ult_fecha_cashout_savings != coalesce(previous_ult_fecha_cashout_savings,"null") then true else false end as enviar_payload_ult_fecha_cashout_savings
        ,case when ult_fecha_cashout_tyba != coalesce(previous_ult_fecha_cashout_tyba,"null") then true else false end as enviar_payload_ult_fecha_cashout_tyba
        ,case when ult_fecha_cashin_tyba != coalesce(previous_ult_fecha_cashin_tyba,"null") then true else false end as enviar_payload_ult_fecha_cashin_tyba
        ,case when cliente_ready != previous_cliente_ready then true else false end as enviar_payload_cliente_ready
        ,case when solicito_tarjeta_fisica != coalesce(previous_solicito_tarjeta_fisica,"null") then true else false end as enviar_payload_solicito_tarjeta_fisica
        ,case when estado_solicitud_tarjeta_fisica != coalesce(previous_estado_solicitud_tarjeta_fisica,"null") then true else false end as enviar_payload_estado_solicitud_tarjeta_fisica
        ,case when paso_funnel_crossborder != coalesce(previous_comercio_recurrente,"null") then true else false end as enviar_payload_paso_funnel_crossborder
        ,case when comercio_recurrente != coalesce(cast(previous_comercio_recurrente as string),"null") then true else false end as enviar_payload_comercio_recurrente
        ,case when comercio_monto != coalesce(cast(previous_comercio_monto as string),"null") then true else false end as enviar_payload_comercio_monto
        ,case when cliente_aleatorio != coalesce(cast(previous_cliente_aleatorio as string),"null") then true else false end as enviar_payload_cliente_aleatorio
        ,case when vinculo_paypal_app != coalesce(cast(previous_vinculo_paypal_app as string),"null") then true else false end as enviar_payload_vinculo_paypal_app
        ,case when cliente_comercio_3M != coalesce(cast(previous_cliente_comercio_3M as string),"null") then true else false end as enviar_payload_cliente_comercio_3M

      FROM pre_target

    ),  
    
    target AS 

    (SELECT
        *,
        case when 
            enviar_payload_state is false AND
            enviar_payload_no_molestar is false AND
            enviar_payload_saldo is false AND
            enviar_payload_saldo_bolsillo is false AND
            enviar_payload_ult_fecha_compra_mastercard is false AND
            enviar_payload_ult_fecha_compra_mastercard_fisica is false AND
            enviar_payload_ult_fecha_crossborder is false AND
            enviar_payload_ult_fecha_top_ups is false AND
            enviar_payload_ult_fecha_utility_payments is false AND
            enviar_payload_ult_fecha_cashin is false AND
            enviar_payload_ult_fecha_cashout is false AND 
            enviar_payload_ult_fecha_p2p_enviado is false AND 
            enviar_payload_ult_fecha_p2p_recibido is false AND
            enviar_payload_ult_fecha_paypal	is false AND
            enviar_payload_ult_fecha_cashin_savings	is false AND
            enviar_payload_ult_fecha_cashout_savings is false AND
            enviar_payload_ult_fecha_cashout_tyba is false AND
            enviar_payload_ult_fecha_cashin_tyba is false AND
            enviar_payload_cliente_ready is false AND 
            enviar_payload_solicito_tarjeta_fisica is false AND 
            enviar_payload_estado_solicitud_tarjeta_fisica is false AND 
            enviar_payload_paso_funnel_crossborder is false AND 
            enviar_payload_comercio_recurrente is false AND 
            enviar_payload_comercio_monto is false AND
            enviar_payload_cliente_aleatorio is false AND 
            enviar_payload_vinculo_paypal_app is false AND 
            enviar_payload_cliente_comercio_3M is false
        THEN false ELSE true END AS envio_payload
    FROM filter_data
    WHERE true
    QUALIFY ROW_NUMBER() OVER (PARTITION BY identity ORDER BY fecha_ejec DESC) = 1
    )

select 
        *,
from target
where envio_payload is true

);


--- Generar tabla
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}` AS (
    with data as (
        select
            users.email as identity
            ,users.fecha_ejec
            ,users.state
            ,CLEVERTAP_TIME(users.date_of_birth)  as date_of_birth
            ,IF(c.no_molestar IS NULL, 'false', c.no_molestar) no_molestar
            ,d.saldo
            ,e.saldo_bolsillo
            ,f.solicito_tarjeta_fisica
            ,g.estado_solicitud_tarjeta_fisica
            ,m.paso_funnel_crossborder
            ,n.comercio_recurrente
            ,o.comercio_monto
            ,CLEVERTAP_TIME(h.ult_fecha_compra_mastercard) as ult_fecha_compra_mastercard
            ,CLEVERTAP_TIME(h.ult_fecha_compra_mastercard_fisica) as ult_fecha_compra_mastercard_fisica
            ,CLEVERTAP_TIME(h.ult_fecha_crossborder) as ult_fecha_crossborder
            ,CLEVERTAP_TIME(h.ult_fecha_top_ups) as ult_fecha_top_ups
            ,CLEVERTAP_TIME(h.ult_fecha_utility_payments) as ult_fecha_utility_payments
            ,CLEVERTAP_TIME(h.ult_fecha_cashin) as ult_fecha_cashin
            ,CLEVERTAP_TIME(h.ult_fecha_cashout) as ult_fecha_cashout
            ,CLEVERTAP_TIME(h.ult_fecha_p2p_enviado) as ult_fecha_p2p_enviado
            ,CLEVERTAP_TIME(h.ult_fecha_p2p_recibido) as ult_fecha_p2p_recibido
            ,CLEVERTAP_TIME(h.ult_fecha_paypal) as ult_fecha_paypal
            ,CLEVERTAP_TIME(h.ult_fecha_cashin_savings) as ult_fecha_cashin_savings
            ,CLEVERTAP_TIME(h.ult_fecha_cashout_savings) as ult_fecha_cashout_savings
            ,CLEVERTAP_TIME(h.ult_fecha_cashout_tyba) as ult_fecha_cashout_tyba
            ,CLEVERTAP_TIME(h.ult_fecha_cashin_tyba) as ult_fecha_cashin_tyba
            ,coalesce(j.cliente_ready, true) as cliente_ready
            ,coalesce(p.cliente_aleatorio, NULL) cliente_aleatorio
            ,coalesce(CAST(q.vinculo_paypal_app AS STRING), NULL) vinculo_paypal_app
            ,coalesce(CAST(r.cliente_comercio_3M AS STRING), NULL) cliente_comercio_3M
             
        from `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` users
        join `${project_target}.temp.clevertap_injection_envio_payload_final_{{period}}_{{ds_nodash}}` envio_payload on users.email = envio_payload.identity 
        left join `${project_target}.temp.clevertap_injection_{{period}}_user_touch_policy_{{ds_nodash}}` c using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_users_balance_{{ds_nodash}}` d using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_users_savings_balance_{{ds_nodash}}` e using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_last_fechas_economics` h using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_cliente_ready_{{ds_nodash}}` j using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_solicito_tf_{{ds_nodash}}` f using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_tf_{{ds_nodash}}` g using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_estado_funnel_paypal_{{ds_nodash}}` m using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_comercio_recurrente_{{ds_nodash}}` n using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_comercio_monto_{{ds_nodash}}` o using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_crm_cliente_aleatorio_{{ds_nodash}}` p using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_paypal_vinculo_app_{{ds_nodash}}` q using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_comercio_3m_{{ds_nodash}}` r using (email)
    
    )

    SELECT
    identity
    ,fecha_ejec
    ,to_json_string((
        SELECT AS STRUCT * EXCEPT(identity, fecha_ejec)
        FROM UNNEST([t])
    )) as profileData
    from data t

);