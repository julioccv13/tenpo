--- Funciones auxiliares
CREATE TEMP FUNCTION CLEVERTAP_TIME(UNIX_TIME INT64)
  RETURNS STRING
  AS (
      IF(UNIX_TIME IS NOT NULL, '$D_' || CAST(CAST(UNIX_TIME AS INT64) AS STRING), NULL)
    );

CREATE TEMPORARY FUNCTION capitalize(str STRING)
RETURNS STRING
LANGUAGE js AS """
  return str.replace(
      /\\w\\S*/g,
      function(txt) {
          return txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase();
      }
  );
""";

--- 1 universo de usuarios
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_CRS_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_CRS_{{ds_nodash}}`  AS (
    SELECT DISTINCT
        a.tenpo_uuid id,
        b.email,
        cnt_trx_en_rubro_top,
        rubro_top, 
        cnt_unique_rubro,
    FROM `${project_source4}.activity.top_industry`  a
        JOIN  `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` b 
        ON a.tenpo_uuid = b.user
    WHERE true
    QUALIFY row_number() over (partition by a.tenpo_uuid order by cnt_trx_en_rubro_top desc) = 1
);

DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_RFMP_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_RFMP_{{ds_nodash}}` AS (
  SELECT DISTINCT
    a.user id,
    b.email,
    UNIX_SECONDS(TIMESTAMP(Fecha_Fin_Analisis_DT)) as ultima_fecha_rfmp_60,
    segment_ult60d as ultimo_segmento_rfmp_60,
    recency,
    cuenta_trx_origen as frequency,
  FROM `${project_target}.tablones_analisis.tablon_rfmp_v2` a
    JOIN  `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` b 
    ON a.user = b.user
  WHERE true
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.user ORDER BY Fecha_Fin_Analisis_DT DESC) = 1
);

--- tabla de oportunista
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_oportunistas_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_oportunistas_{{ds_nodash}}` AS (
  
  
WITH target AS

  (SELECT
      user,
      email,
      ROUND(SUM(monto)) as gpv,
  FROM `${project_source4}.economics.economics` 
  WHERE nombre NOT LIKE '%Devolución%'
  AND linea in ('utility_payments','top_ups','mastercard_physical','paypal','mastercard')
  GROUP BY 1,2),

hooks AS 

(SELECT 
    user,
    ROUND(SUM(monto)) as gpv_hooks
FROM `${project_source4}.economics.economics` 
WHERE linea = 'reward'
GROUP BY 1 )

SELECT
        target.user,
        target.email,
        target.gpv,
        IFNULL(hooks.gpv_hooks,0) gpv_hooks,
        CASE
            WHEN IFNULL(hooks.gpv_hooks,0)/target.gpv >= 0.5 THEN true
        ELSE false END AS es_oportunista
FROM target
JOIN  `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` USING (user)
LEFT JOIN hooks USING (user)


);


-- Tabla cross sell
DROP TABLE IF EXISTS `${project_target}.temp.rfm_cross_sell`;
CREATE TABLE`${project_target}.temp.rfm_cross_sell` AS (
SELECT
    *,
     `${project_target}.aux_table.DISTINCT_ARR`(ARRAY(SELECT * FROM UNNEST(SPLIT(SUBSTR(productos_origen, 2 , LENGTH(productos_origen) - 2))))) AS unique_products,
FROM `${project_target}.tablones_analisis.tablon_rfmp_v2`
WHERE Fecha_Fin_Analisis_DT = (SELECT MAX(Fecha_Fin_Analisis_DT) FROM `${project_target}.tablones_analisis.tablon_rfmp_v2` )
AND cuenta_trx_origen > 5
AND  uniq_productos_origen < 6
AND recency <= 30
);


--- Property de cross_sell
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_cross_sell_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_cross_sell_{{ds_nodash}}` AS (

WITH target AS

(SELECT 
      DISTINCT 
      email,
      productos
FROM `${project_target}.temp.rfm_cross_sell`
CROSS JOIN UNNEST(unique_products) as productos
ORDER BY 1 DESC),

mastercard AS (

    SELECT
        DISTINCT email,
        1 as uso_mastercard
    FROM target
    where productos like '%mastercard%'

),

utility_payments AS (

    SELECT
        DISTINCT email,
        1 as uso_utility_payments
    FROM target
    where productos like '%utility_payments%'

),

top_ups AS (

    SELECT
        DISTINCT email,
        1 as uso_top_ups
    FROM target
    where productos like '%top_ups%'

),

paypal AS (

        SELECT
        DISTINCT email,
        1 as uso_paypal
    FROM target
    where productos like '%paypal%'

), remesas as (

        SELECT
        DISTINCT email,
        1 as uso_remesas
    FROM target
    where productos like '%cross%'

),

summary AS (

select 
    distinct 
        email,
        IFNULL(mastercard.uso_mastercard,0) uso_mastercard,
        IFNULL(utility_payments.uso_utility_payments,0) uso_utility_payments,
        IFNULL(top_ups.uso_top_ups,0) uso_top_ups,
        IFNULL(paypal.uso_paypal,0) uso_paypal,
        IFNULL(remesas.uso_remesas,0) uso_remesas
from target
left join mastercard using (email)
left join utility_payments using (email)
left join top_ups using (email)
left join paypal using (email)
left join remesas using (email)

),

xsell AS (

SELECT 
    *,
    CASE
        WHEN uso_mastercard = 0 THEN 'mastercard'
        WHEN uso_utility_payments = 0 THEN 'utility_payments'
        WHEN uso_top_ups = 0 THEN 'top_ups'
        WHEN uso_paypal = 0 THEN 'paypal'
        WHEN uso_remesas = 0 THEN 'remesas'
    END AS cross_sell
FROM summary

)

SELECT 
    xsell.*
FROM xsell
JOIN `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` USING (email)
WHERE cross_sell IS NOT NULL

);

--- Bancos cashin
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_banks_cashin_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_banks_cashin_{{ds_nodash}}`AS (

    SELECT
        DISTINCT
        user,
        B.email,
        replace(cast(to_json_string(`${project_target}.aux_table.DISTINCT_ARR`(array_agg(capitalize(cico_banco_tienda_origen)) over (partition by user))) as string),"/",'-') AS bancos_cashin 
        FROM `${project_source4}.bancarization.cca_cico_tef` A
    JOIN `${project_source1}.users.users` B on A.user = B.id
    WHERE tipo_trx = 'cashin'
    AND lower(cico_banco_tienda_origen) NOT LIKE '%tenpo%'
    
);

--- generar tabla
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_pre_dataset`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_pre_dataset` AS (
    
    with data as (
        select
            a.email as identity
            ,a.fecha_ejec

            ,c.ultimo_segmento_rfmp_60 as rfmp_segment_ult60d
            ,CAST(c.recency AS INT64) recency
            ,CAST(c.frequency AS INT64)  frequency

            ,IF(b.cnt_trx_en_rubro_top IS NULL, 0, b.cnt_trx_en_rubro_top) as trx_top_rubro
            ,IF(b.rubro_top IS NULL, 'N/A', b.rubro_top) top_rubro
            ,IF(b.cnt_unique_rubro IS NULL, 0, b.cnt_unique_rubro) as cnt_unique_rubro
            ,IFNULL(d.es_oportunista, false) es_oportunista
            ,IFNULL(e.cross_sell, 'N/A') cross_sell
            ,f.bancos_cashin

        from `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` a
        left join `${project_target}.temp.clevertap_injection_{{period}}_CRS_{{ds_nodash}}` b using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_RFMP_{{ds_nodash}}` c using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_oportunistas_{{ds_nodash}}` d using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_cross_sell_{{ds_nodash}}`e using (email)
        left join `${project_target}.temp.clevertap_injection_{{period}}_banks_cashin_{{ds_nodash}}` f using (email)
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


--- Tabla final
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}` AS (


    SELECT
        identity,
        fecha_ejec,
        replace(replace (replace(profileData,'\\',"") , "\"[","["),"\"}","}") as profileData
    FROM `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}_pre_dataset`

);