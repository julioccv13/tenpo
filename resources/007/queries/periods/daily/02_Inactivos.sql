/*

INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}` -- Insert En
SELECT 
    --DISTINCT -- TODO: Revisar como hacerlo sin distinct 
    email as identity,
    UNIX_SECONDS(TIMESTAMP(date(submitted_at))) + 43200 as ts,
    'NPS' as evtName,
    to_json_string(struct('flujos' as Type, flows.source as Source, CAST(score AS INT64) as score)) as evtData
FROM `tenpo-it-analytics.app.flow_evaluations` flows
JOIN `tenpo-private.identity.ruts` ruts on ruts.rut_complete = flows.rut
JOIN `tenpo-airflow-prod.users.users` users using (tributary_identifier)
WHERE DATE(submitted_at,"America/Santiago")  = '{{ds}}'
AND feedback IS NULL;

*/
INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}` 
SELECT  DISTINCT 
        B.email as identity
        ,UNIX_SECONDS(TIMESTAMP(date(A.fecha_envio))) + 43200 as ts
        ,'Inactivo' as evtName
        ,'{{}}' as evtData
FROM `{project_source_3}.aux.clientes_inactivos` A
JOIN `{project_source_1}.users.users` B on A.user = B.id
WHERE A.fecha_envio = CAST(DATE_ADD(PARSE_DATE("%Y%m%d", "{{ds_nodash}}"), INTERVAL 1 DAY) AS DATETIME) --inactivos de hoy

