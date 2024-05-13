{{ config(materialized='table') }}

        with sessions_lenght as 
                
                (select 
                distinct
                fecha_hora,
                date(fecha_hora) date,
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
                distinct 
                *,
                round(avg(session_lenght) over (partition by year),2) avg_session_lenght,
                percentile_disc(session_lenght, 0.5) over (partition by  year) median_session_lenght
        from sessions_lenght
        order by year desc