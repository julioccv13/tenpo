{{ 
    config(
        materialized='table'
        ,tags=["daily", "bi", "datamart"]
        ,project=env_var('DBT_PROJECT24', 'tenpo-datalake-sandbox')
    )
}}

select distinct 
  id, 
  email, 
  rut, 
  state,
  state_name,
  description as state_description
from {{ source('tenpo_users', 'users') }}
  left join  {{ref('user_states')}} using (state)