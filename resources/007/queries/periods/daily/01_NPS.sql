INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}`
WITH score as (
    SELECT
        answer as score,
        item_id,
        DATE(submitted_at,"America/Santiago") AS submitted_at
    FROM `{project_source_2}.typeform.typeform_item_answer`
    where form_id = 'yXYjLgjX'
    and field_type = 'opinion_scale'
    order by item_id desc
), user as (
        SELECT
        REPLACE(REPLACE(answer, "87356-445960001-30-7657398233-49876-456765-98785324-set",""),"f809021-898765678765","") as rut,
        item_id,
        submitted_at
    FROM `{project_source_2}.typeform.typeform_item_answer`
    where form_id = 'yXYjLgjX'
    and field_type = 'hidden'
    order by item_id desc
)

SELECT 
    --DISTINCT -- TODO: Revisar como hacerlo sin distinct 
    users.email as identity,
    UNIX_SECONDS(TIMESTAMP(score.submitted_at)) + 43200 as ts,
    'NPS' as evtName,
    to_json_string(struct('general' as Type, CAST(score.score AS INT64) as score)) as evtData
FROM score
JOIN user using (item_id)
JOIN `{project_source_4}.identity.ruts`ruts on ruts.rut_complete	= user.rut
JOIN `{project_source_1}.users.users` users using (tributary_identifier)
WHERE score.submitted_at = '{{ds}}'; -- TODO: esto mejor hacerlo con una sola tabla de parametros

INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}`
SELECT 
    --DISTINCT -- TODO: Revisar como hacerlo sin distinct 
    email as identity,
    UNIX_SECONDS(TIMESTAMP(date(submitted_at))) + 43200 as ts,
    'NPS' as evtName,
    to_json_string(struct('flujos' as Type, flows.source as Source, CAST(score AS INT64) as score)) as evtData
FROM `{project_source_5}.app.flow_evaluations` flows
JOIN `{project_source_4}.identity.ruts` ruts on ruts.rut_complete = flows.rut
JOIN `{project_source_1}.users.users` users using (tributary_identifier)
WHERE DATE(submitted_at,"America/Santiago")  = '{{ds}}'
AND feedback IS NULL;

INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}`
SELECT 
    --DISTINCT -- TODO: Revisar como hacerlo sin distinct 
    email as identity,
    UNIX_SECONDS(TIMESTAMP(date(submitted_at))) + 43200 as ts,
    'NPS' as evtName,
    to_json_string(struct('flujos' as Type, flows.source as Source, CAST(score AS INT64) as score, flows.feedback as feedback, ROUND(flows.sentiment_score,2) as sentiment)) as evtData
FROM `{project_source_5}.app.flow_evaluations` flows
JOIN `{project_source_4}.identity.ruts` ruts on ruts.rut_complete = flows.rut
JOIN `{project_source_1}.users.users` users using (tributary_identifier)
WHERE DATE(submitted_at,"America/Santiago")  = '{{ds}}' -- User parametros
AND feedback IS NOT NULL;
