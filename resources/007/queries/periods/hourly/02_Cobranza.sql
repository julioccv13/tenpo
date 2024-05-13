INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}`
SELECT  
        DISTINCT
        lower(A.correo) identity
        ,UNIX_SECONDS(TIMESTAMP(date(current_date()))) + 43200 as ts
        ,'Inscripcion campana' as evtName
        ,to_json_string(struct('cobranza' as name,'paypal' as tipo)) as evtData
FROM `{project_source_3}.aux.comunicacion_cobranza` A
JOIN `{project_source_1}.users.users` B on lower(A.correo) = B.email
WHERE DATE(A.date) = DATE(CURRENT_DATE());
