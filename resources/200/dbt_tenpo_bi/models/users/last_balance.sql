{{ config(materialized='table') }}

-- TODO: ¿En que se usa esta tabla? 
WITH last_balance AS
(SELECT 
    MAX(fecha) OVER (PARTITION BY email) as ultima_fecha,
    fecha,
    {{ hash_sensible_data('u.email') }} as email, -- TODO: Adri, ¿Es necesario aca el Email o podria ser el Tenpo UUID?
    SaldoConciliado,
    SaldoAPP	
--FROM `tenpo-airflow-prod.users.saldos` as s
FROM {{ source('tenpo_users', 'saldos') }} as s
JOIN {{ source('tenpo_users', 'users') }} as u ON u.tributary_identifier = s.rut_hash)

SELECT DISTINCT * EXCEPT (ultima_fecha)  FROM last_balance
WHERE ultima_fecha = fecha
ORDER BY FECHA DESC