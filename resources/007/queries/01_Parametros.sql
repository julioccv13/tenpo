DROP TABLE IF EXISTS `{project_id}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}`;
CREATE TABLE `{project_id}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}` AS (
    SELECT
    CAST(DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 1 DAY) AS DATETIME) AS fecha_ejec
);


DROP TABLE IF EXISTS `{project_id}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}`;
CREATE TABLE `{project_id}.temp.clevertap_injection_{{period}}_target_users_{{ds_nodash}}` AS (

    SELECT
        distinct
        id as user,
        A.tributary_identifier,
        a.email,
        ob_completed_at,
        fecha_ejec,
        UNIX_SECONDS(date_of_birth) + 43200 as date_of_birth
    FROM `{project_source_1}.users.users` A
    INNER JOIN  `{project_source_2}.clevertap_audit.clevertap_users_v2` B
    ON A.email = B.email
    LEFT JOIN `{project_id}.temp.clevertap_injection_{{period}}_params_{{ds_nodash}}`
    ON 1=1
    where state in (4,7,8)
);

DROP TABLE IF EXISTS `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}`;
CREATE TABLE `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}` (
    identity STRING
    ,ts INT64
    ,evtName STRING
    ,evtData STRING
);
