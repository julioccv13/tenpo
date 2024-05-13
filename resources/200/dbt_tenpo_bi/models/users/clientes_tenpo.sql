{{ config(materialized='table') }}

-- TODO: Cual es la diferencia entre esta y la users_allservices?
SELECT DISTINCT
         id,
         tributary_identifier as rut,
         state,
         {{ hash_sensible_data('phone') }} as phone,
         {{ hash_sensible_data('email') }} as email,
         created_at as ts_creacion_tenpo,
         ob_completed_at as ts_ob_tenpo,
         FROM
         {{ source('tenpo_users', 'users') }}