INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}`
SELECT  
        DISTINCT  
        A.identity
        ,UNIX_SECONDS(TIMESTAMP(date(current_date()))) + 43200 as ts
        ,'Inscripcion campana' as evtName
        ,to_json_string(struct('solicitud nueva direccion' as name)) as evtData
FROM `{project_source_3}.aux.comunicacion_solicitud_nueva_direccion_tf` A
JOIN `{project_source_1}.users.users` B on A.identity = B.email
WHERE DATE(A.date) = DATE(CURRENT_DATE());
