--- 1 universo de usuarios
CREATE TEMP FUNCTION CLEVERTAP_TIME(UNIX_TIME INT64)
  RETURNS STRING
  AS (
      IF(UNIX_TIME IS NOT NULL, '$D_' || CAST(CAST(UNIX_TIME AS INT64) AS STRING), NULL)
    );

DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}` AS (
    SELECT
    CAST(DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 1 DAY) AS DATETIME) AS fecha_ejec
);

--- bloqueos
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_bloqueos_clevertap_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_bloqueos_clevertap_{{ds_nodash}}` AS (

    SELECT 
        DISTINCT 
            email as identity
    FROM `${project_source1}.users.users`
    WHERE state IN (5 ,6,7,8,17,18,19,20,21,22,23,24)
);

--- desbloqueos
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_desbloqueos_clevertap_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_desbloqueos_clevertap_{{ds_nodash}}` AS (

    SELECT 
        DISTINCT 
            email as identity
    FROM `${project_source1}.users.users`
    WHERE state = 4
);

--- regularizaciones (cobranza)
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_cobranza_clevertap_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_cobranza_clevertap_{{ds_nodash}}` AS (

    WITH data AS 

        (SELECT
            date,
            DATETIME(
                CAST(RIGHT(fecha,4) AS INT64),
                CAST(LEFT(RIGHT(fecha, length(fecha) - STRPOS(fecha, '/') ) ,  STRPOS( RIGHT(fecha, length(fecha) - STRPOS(fecha, '/') ) , '/' ) - 1 ) AS INT64),
                CAST(LEFT(fecha,STRPOS(fecha, '/')-1) AS INT64),
                0,0,0
            ) fecha,
            monto,
            caso,
            LOWER(correo) email
        FROM `${project_target}.aux.comunicacion_cobranza`)

        SELECT
            
            email as identity,
            CLEVERTAP_TIME(UNIX_SECONDS(timestamp(fecha))) as ult_fecha_regularizacion,
            monto as monto_regularizar

        FROM data
        WHERE date(date) = date(current_date())


);

--- union
DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_{{ds_nodash}}` AS (


WITH bloqueos AS (SELECT
        
        DISTINCT 
            identity
            ,false as MSG_push
            ,false as MSG_sms
            ,false as MSG_whatsapp
            ,false as MSG_dndPhone
            ,false as MSG_email
    FROM  ${project_target}.temp.clevertap_injection_{{period}}_bloqueos_clevertap_{{ds_nodash}}
    
), desbloqueos AS (SELECT

    DISTINCT 
            identity
            ,true as MSG_push
            ,true as MSG_sms
            ,true as MSG_whatsapp
            ,true as MSG_dndPhone
            ,true as MSG_email
    FROM  ${project_target}.temp.clevertap_injection_{{period}}_desbloqueos_clevertap_{{ds_nodash}}

), cobranza AS (

    select 
        identity
        ,to_json_string((
            SELECT AS STRUCT * EXCEPT(identity)
            FROM UNNEST([t])
        )) as profileData
    from  ${project_target}.temp.clevertap_injection_{{period}}_cobranza_clevertap_{{ds_nodash}} t

    
)
, data as (

    SELECT * FROM bloqueos
    UNION ALL
    SELECT * FROM desbloqueos

)

,payload as (

    SELECT
    identity
    ,to_json_string((
        SELECT AS STRUCT * EXCEPT(identity)
        FROM UNNEST([t])
    )) as profileData

FROM  data t

UNION ALL

SELECT * FROM cobranza

)

SELECT 
    identity
    ,fecha_ejec
    ,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(profileData,'MSG_push','MSG-push'),'MSG_sms','MSG-sms'),'MSG_whatsapp','MSG-whatsapp'),'MSG_dndPhone','MSG-dndPhone'),'MSG_email','MSG-email') as profileData
FROM payload
LEFT JOIN ${project_target}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}
ON 1=1

);