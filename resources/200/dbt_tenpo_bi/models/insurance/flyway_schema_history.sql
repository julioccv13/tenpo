{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT installed_rank,version,description,type,script,checksum,installed_by,installed_on,execution_time,success FROM {{ source('insurance', 'flyway_schema_history') }} 

