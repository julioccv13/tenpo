/* GRUPO CERO - Telefonos*/
DROP TABLE IF EXISTS `${project_target}.segments.rfm_grupo_cero`;
CREATE TABLE`${project_target}.segments.rfm_grupo_cero` AS (

WITH phones AS (SELECT 
                    CAST(CONCAT('569',phone) AS STRING) phone
FROM `${project_source_1}.firestore_export.tenpo_cl_sms_v3_complete`)

SELECT  DISTINCT 
        phones.phone AS celular,
        B.*
FROM phones
LEFT JOIN `${project_source_2}.users.users` B USING (phone)
WHERE B.id IS NULL

);



/* GRUPO CERO OB INCOMPLETOS*/
DROP TABLE IF EXISTS `${project_target}.segments.rfm_grupo_ob_incompleto`;
CREATE TABLE`${project_target}.segments.rfm_grupo_ob_incompleto` AS (
SELECT * 
FROM `${project_source_2}.users.users`
WHERE state NOT IN
    (4,
    6,
    5,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    24)
     AND safe_cast(phone as string) NOT IN (

         SELECT DISTINCT safe_cast(celular as string) FROM `${project_target}.segments.rfm_grupo_cero`

     )
);


/* OB SIN MOVIMIENTOS */

DROP TABLE IF EXISTS `${project_target}.segments.rfm_grupo_ob_sin_movimientos`;
CREATE TABLE`${project_target}.segments.rfm_grupo_ob_sin_movimientos` AS (
    
    SELECT DISTINCT user
    FROM `${project_source_3}.churn.daily_churn` 
    WHERE state = 'activo'
    AND user NOT IN (
        
        SELECT 
                DISTINCT user
        FROM `${project_source_3}.economics.economics`
    )
);


/*OB con FCI O P2P recibido*/

DROP TABLE IF EXISTS `${project_target}.segments.rfm_grupo_fci_sin_movimientos`;
CREATE TABLE`${project_target}.segments.rfm_grupo_fci_sin_movimientos` AS (

SELECT DISTINCT user
FROM `${project_source_3}.churn.daily_churn` 
JOIN (SELECT 
            DISTINCT user
    FROM `${project_source_3}.economics.economics` 
    WHERE linea in ('cash_in','p2p_received')

) cashin USING (user)
WHERE state = 'activo'
AND user NOT IN (
    SELECT 
        DISTINCT user
    FROM `${project_source_3}.economics.economics` 
    WHERE linea not in  ('cash_in','p2p_received','paypal')
    )
);

-- SELECT COUNT(DISTINCT user) FROM  `${project_target}.segments.rfm_grupo_fci_sin_movimientos`;


/*Cross sell*/
-- DROP TABLE IF EXISTS `${project_target}.segments.rfm_cross_sell`;
-- CREATE TABLE`${project_target}.segments.rfm_cross_sell` AS (

--     SELECT 
--         user,
--         count(trx_id) transactions
--     FROM `${project_source_3}.economics.economics` 
--     WHERE linea in ('cash_in','cash_out','p2p','p2p_received','crossborder')
--     AND user NOT IN (
--         SELECT 
--             DISTINCT
--             user,
--         FROM `${project_source_3}.economics.economics` 
--         WHERE linea in  ('mastercard',
--                         'mastercard_physical',
--                         'utility_payments',
--                         'top_ups',
--                         --'p2p',
--                         --'p2p_received',
--                         'paypal',
--                         --'crossborder',
--                         'cash_in_savings'))
--     AND user NOT IN (SELECT DISTINCT user FROM `${project_target}.segments.rfm_grupo_fci_sin_movimientos`) -- excluir grupo anterior (solo cashin)
--     GROUP BY user
--     HAVING count(trx_id) > 4
-- );

-- SELECT COUNT(DISTINCT user) FROM `${project_target}.segments.rfm_cross_sell`;

-- Segundo cross sell
DROP TABLE IF EXISTS `${project_target}.segments.rfm_cross_sell`;
CREATE TABLE`${project_target}.segments.rfm_cross_sell` AS (
SELECT
    *,
     `${project_source_4}.aux_table.DISTINCT_ARR`(ARRAY(SELECT * FROM UNNEST(SPLIT(SUBSTR(productos_origen, 2 , LENGTH(productos_origen) - 2))))) AS unique_products,
FROM `${project_source_4}.tablones_analisis.tablon_rfmp_v2`
WHERE Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM `${project_source_4}.tablones_analisis.tablon_rfmp_v2` )
AND cuenta_trx_origen > 5
AND  uniq_productos_origen < 6
AND recency <= 30
);






/* Publico objetivo */
DROP TABLE IF EXISTS `${project_target}.segments.rfm`;
CREATE TABLE `${project_target}.segments.rfm` AS (

WITH rfm AS
(SELECT
    DISTINCT 
    user,
    CASE 
        WHEN recency > 0 AND recency <= 30 THEN '1. 1 A 30'
        WHEN recency > 30 AND recency <= 60 THEN '2. 31 A 60'
        WHEN recency > 60 AND recency <= 90 THEN '3. 61 A 90'
    ELSE '4. MAYOR A 90' END AS recencia_tag,
    segment_ult60d
FROM `${project_source_4}.tablones_analisis.tablon_rfmp_v2` 
WHERE Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM `${project_source_4}.tablones_analisis.tablon_rfmp_v2` )
AND recency	IS NOT NULL),

frecuency AS 
(SELECT
    DISTINCT
    user,
    CASE 
        WHEN cuenta_trx_origen = 1 THEN '1 TRX'
        WHEN cuenta_trx_origen > 1 AND cuenta_trx_origen <= 5 THEN '2 A 5'
        WHEN cuenta_trx_origen > 6 AND cuenta_trx_origen <= 10 THEN '6 A 10 TRX'
    ELSE 'MAYOR A 10' END AS transaccionalidad_tag
FROM `${project_source_4}.tablones_analisis.tablon_rfmp_v2`
WHERE Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM `${project_source_4}.tablones_analisis.tablon_rfmp_v2` )
AND cuenta_trx_origen IS NOT NULL),

publico AS (SELECT 
    rfm.*, frecuency.transaccionalidad_tag
FROM rfm
LEFT JOIN frecuency USING (user)
WHERE rfm.user NOT IN (
    SELECT DISTINCT user 
    FROM `${project_target}.segments.rfm_cross_sell`
    UNION ALL
    SELECT DISTINCT user
    FROM `${project_target}.segments.rfm_grupo_fci_sin_movimientos`
    UNION ALL
    SELECT DISTINCT user
    FROM `${project_target}.segments.rfm_grupo_ob_sin_movimientos`
))

SELECT 
    DISTINCT *
FROM publico

);



/* Filtro oportunista */
DROP TABLE IF EXISTS `${project_target}.segments.rfm_oportunistas`;
CREATE TABLE `${project_target}.segments.rfm_oportunistas` AS (


WITH target AS 
(SELECT 
    user,
    ROUND(SUM(monto)) as gpv
FROM `${project_source_3}.economics.economics` 
WHERE nombre NOT LIKE '%Devolución%'
AND linea in ('utility_payments','top_ups','mastercard_physical','paypal','mastercard')
GROUP BY 1),

hooks AS (
SELECT 
    user,
    ROUND(SUM(monto)) as gpv_hooks
FROM `${project_source_3}.economics.economics` 
WHERE linea = 'reward'
GROUP BY 1 )

SELECT
        target.*,
        IFNULL(hooks.gpv_hooks,0) gpv_hooks,
        CASE
            WHEN IFNULL(hooks.gpv_hooks,0)/target.gpv >= 0.5 THEN true
        ELSE false END AS is_oportunista
FROM target
LEFT JOIN hooks USING (user)

);
