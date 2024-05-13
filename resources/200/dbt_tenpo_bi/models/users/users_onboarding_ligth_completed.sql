{{ config(tags=["hourly", "bi"], materialized='ephemeral') }}

WITH data AS 
(select
    a.*
    from `tenpo-airflow-prod.users.onboarding_PROD` a
    join `tenpo-airflow-prod.users.users` b on a.user_id = b.id
    where status is not null
    and a.user_id is not null 
    and a.user_new is not null 
    and a.phone is not null 
    and a.email is not null 
    and a.azure_id is not null 
    and a.updated is not null 
    and a.created is not null 
    and a.email_provider is not null 
)

SELECT
    *
FROM data
qualify row_number() over (partition by user_id order by updated desc) = 1
