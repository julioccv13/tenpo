{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}

select distinct 
    u.*,
    ub.* except (user_id,id,created_at,updated_at),
    ub.id as id_user_onboarding,
    ub.created_at as created_at_user_onboarding,
    ub.updated_at as updated_at_user_onboarding,
    first_value(ub.origin) over (partition by ub.user_id order by ub.created_at asc 
      rows between unbounded preceding and unbounded following) as source
from `tenpo-airflow-prod.onboarding_savings.users` u 
join `tenpo-airflow-prod.onboarding_savings.user_onboarding` ub on (u.id = ub.user_id)