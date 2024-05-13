{{ 
    config(
        materialized='table'
        ,tags=["daily", "bi"]
        ,project=env_var('DBT_PROJECT19', 'tenpo-datalake-sandbox')
        ,schema='external'
        ,alias='advertising_id_last_6m'
    )
}}

select distinct advertising_id from (
SELECT 
distinct device.advertising_id, STRING_AGG(DISTINCT  event_name,',') events
FROM {{source('analytics_185654164','events')}} -- `mine-app-001.analytics_185654164.events_*` 
where 
device.advertising_id is not null and device.advertising_id <> ' ' and device.advertising_id <> '' 
and PARSE_DATE('%Y%m%d',  event_date) > date_sub(current_date(), interval 180 day) group by 1 
) where events like '%first_open%' and events not like '%ob_successful%'
