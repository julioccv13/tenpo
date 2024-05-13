{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}

select distinct
  tributary_identifier,
  mov.* except (tenpo_id)
from {{source('tyba_tenpo','ccam_pending_movements')}}  mov
  join {{ref('users_savings')}}  usr on  (mov.tenpo_id = usr.id)