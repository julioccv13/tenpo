{{ 
    config(
        materialized='table'
        ,tags=["daily", "bi", "datamart"]
        ,project=env_var('DBT_PROJECT11', 'tenpo-datalake-sandbox')
    )
}}

select  distinct
    tributary_identifier,
    t1.rut,
    * except (rut,nombre_razon_social,rut_complete,dv,tributary_identifier,estimated_yob),
from {{source('scrappers_sii','stc_registro_consultas')}} t1
left join {{source('scrappers_sii','stc_actividades')}}  t2 using (rut,fecha_consulta)
left join {{source('scrappers_sii','stc_timbraje_documentos')}}  t3 using (rut,fecha_consulta)
left join {{source('identity','ruts')}} t4 on (t1.rut = t4.rut_complete)