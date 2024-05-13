INSERT INTO `{project_id}.temp.clevertap_injection_{{period}}_events_{{ds_nodash}}` 
SELECT  
        DISTINCT  
        email as identity
        ,UNIX_SECONDS(TIMESTAMP(date(current_date()))) + 43200 as ts
        ,'Inscripcion campana' as evtName
        ,to_json_string(struct('next best comercio' as name, rubro as category, next_best as comercio)) as evtData
from `{project_source_3}.temp.next_best_comercio_{{ds_nodash}}` A
join `{project_source_1}.users.users` B on A.user = B.id
