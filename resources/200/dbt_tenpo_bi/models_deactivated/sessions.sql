{{ config(materialized='table') }}

with target as 

(with sub_target as 

    (with sessions as (select 
            distinct
            fecha_hora,
            format_date("%Y%m", fecha_hora) year,
            email,
            session_id,
            case when session_lenght = 0 then 1 else session_lenght end as session_lenght
        --from {{source('clevertap','events')}}
        from {{ source('clevertap', 'events') }}
        --join {{source('tenpo_users','users')}}users using (email)
        join {{ source('tenpo_users', 'users') }} using (email) 
        where event = 'Session Concluded'
        and users.state = 4
        order by email, fecha_hora desc, session_id desc)

    select 
            *,
            count(email) over (partition by email) as total_sessions,
            count(email) over (partition by email, format_date("%Y%m", fecha_hora) order by format_date("%Y%m", fecha_hora) desc ) sessions_per_month,
    from sessions) 

select 
        distinct year, email, sessions_per_month
from sub_target
order by year desc) 


select  * , 
        avg(sessions_per_month) over (partition by year order by year desc) sessiones_promedio 
from target order by year desc
