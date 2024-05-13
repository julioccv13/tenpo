DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}` AS (
    SELECT
    CAST(DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 1 DAY) AS DATETIME) AS fecha_ejec
);


DROP TABLE IF EXISTS `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}`;
CREATE TABLE `${project_target}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` AS (

WITH target AS 
    
    (SELECT
        DISTINCT
        created_at,
        id as user,
        A.tributary_identifier,
        LOWER(A.email) email,
        CAST(DATE(ob_completed_at) AS STRING) ob_completed_at,
        fecha_ejec,
        state,
        UNIX_SECONDS(date_of_birth) + 43200 as date_of_birth
    FROM `${project_source1}.users.users` A
    INNER JOIN  `${project_source2}.clevertap_raw.users` B
    ON A.email = B.email
    LEFT JOIN `${project_target}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}`
    ON 1=1
    )

/* vamos a seleccionar los ultimos dos meses por que el proceso se cae */
SELECT * 
FROM target
WHERE state NOT IN (0,1,2,3,9)
LIMIT 10000
-- AND date_trunc(date(ob_completed_at),MONTH) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(),INTERVAL - 1 MONTH), MONTH)
-- AND date_trunc(date(ob_completed_at),MONTH) <= DATE_TRUNC(DATE_ADD(CURRENT_DATE(),INTERVAL + 1 MONTH), MONTH)

);