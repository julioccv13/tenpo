INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}` 
SELECT  
        DISTINCT  
        email as identity
        ,UNIX_SECONDS(TIMESTAMP(date(current_date()))) + 43200 as ts
        ,'Inscripcion campana' as evtName
        ,to_json_string(struct('Solicitud de datos bancarios' as name)) as evtData
FROM `{project_source_3}.temp.solicitud_datos_bancarios`

