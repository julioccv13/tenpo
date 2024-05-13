{% set partitions_to_replace = [
    'current_date',
    'date_sub(current_date, interval 3 day)'
] %}


{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'fecha', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}

with datos as (
  select 
    timestamp
    ,protopayload_auditlog
    ,resource
    ,severity
  from {{ source ('bq_audit_tenpo_airflow_prod','cloudaudit_googleapis_com')}}
  where true
  {% if is_incremental() %}
    and cast(timestamp as date) in ({{ partitions_to_replace | join(',') }})
  {% endif %}
  qualify row_number() over (partition by insertId order by timestamp) = 1
)

select
  protopayload_auditlog.authenticationInfo.principalEmail	as usuario
  ,cast(timestamp as date) as fecha
  ,count(1) as cnt
  ,sum(case when json_extract_scalar(
    protopayload_auditlog.metadataJson,
    "$.jobChange.job.jobStats.queryStats.cacheHit") = 'true' then 1 else 0 end) as querys_cacheadas
  ,sum(case when json_extract_scalar(
    protopayload_auditlog.metadataJson,
    "$.jobChange.job.jobConfig.labels.data_source_id"
  ) = 'scheduled_query' then 1 else 0 end) as scheduled
  ,sum(cast(json_extract_scalar(
    protopayload_auditlog.metadataJson,
    "$.jobChange.job.jobStats.queryStats.totalBilledBytes"
  ) as int64)/1e+12) as teras_procesados
  ,sum(5.0*cast(json_extract_scalar(
    protopayload_auditlog.metadataJson,
    "$.jobChange.job.jobStats.queryStats.totalBilledBytes"
  ) as int64)/1e+12) as dolares
FROM datos
where true
  and resource.type = 'bigquery_project'
  and json_extract_scalar(
        protopayload_auditlog.metadataJson,
        "$.jobChange.after"
      ) = 'DONE' and severity <> 'ERROR'
group by 1, 2