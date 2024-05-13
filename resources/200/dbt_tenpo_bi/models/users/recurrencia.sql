{% set partitions_to_replace = [
    'date_trunc(current_date, month)',
    'date_sub(date_trunc(current_date, month), INTERVAL 1 MONTH)',
] %}


{{ 
  config(
    tags=["daily", "bi"],
    materialized='incremental',
    partition_by = { 'field': 'mes', 'data_type': 'date' },
    incremental_strategy = 'insert_overwrite',
  ) 
}}

WITH 
users as (
    SELECT DISTINCT 
        id as user
        ,DATE_TRUNC(date(ob_completed_At, 'America/Santiago'), MONTH) as mes_ob
    FROM {{ref('users_tenpo')}}
),
economics AS (
    SELECT
        DATE_TRUNC(fecha, MONTH) as mes,
        user,
        max(linea <> 'reward' and nombre not like '%Devol%') f_mau
    FROM {{ref('economics')}}
    WHERE TRUE
    {% if is_incremental() %}
        and DATE_TRUNC(fecha, MONTH) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
    GROUP BY 1,2
    ORDER BY 1 DESC
), 
data AS (
    SELECT
        DISTINCT
        economics.mes,
        users.mes_ob,
        users.user,
        IFNULL(economics.f_mau,false) f_mau_0,
        LEAD(mes) over (partition by user order by mes desc) previous_month,
        DATE_DIFF(mes,last_day(lead(mes) over (partition by user order by mes desc)),DAY) day_diff        
    FROM economics
        LEFT JOIN users USING (user)
    WHERE f_mau is true
    ORDER BY 1 DESC
), 
mau_type AS(
    SELECT
    mes,
    previous_month,
    CASE WHEN day_diff = 1 THEN true ELSE false END as es_consecutivo,
    mes_ob,
    user,
    f_mau_0,
    IFNULL(lead(f_mau_0) over (partition by user order by mes desc),false) as previous_1,
    IFNULL(lead(f_mau_0,2) over (partition by user order by mes desc),false) as previous_2,
    IFNULL(lead(f_mau_0,3) over (partition by user order by mes desc),false) as previous_3, 
    IFNULL(lead(f_mau_0,4) over (partition by user order by mes desc),false) as previous_4,
    IFNULL(lead(f_mau_0,5) over (partition by user order by mes desc),false) as previous_5,
    IFNULL(lead(f_mau_0,6) over (partition by user order by mes desc),false) as previous_6,
    IFNULL(lead(f_mau_0,7) over (partition by user order by mes desc),false) as previous_7,
    IFNULL(lead(f_mau_0,8) over (partition by user order by mes desc),false) as previous_8,
    IFNULL(lead(f_mau_0,9) over (partition by user order by mes desc),false) as previous_9,
    IFNULL(lead(f_mau_0,10) over (partition by user order by mes desc),false) as previous_10,
    IFNULL(lead(f_mau_0,11) over (partition by user order by mes desc),false) as previous_11,
    IFNULL(lead(f_mau_0,12) over (partition by user order by mes desc),false) as previous_12,
    FROM data
    ORDER BY 1 DESC
),
arrg_data AS (
    SELECT 
        *,
        --CASE WHEN f_mau_0 is true THEN 'mau' ELSE 'no_mau' END AS is_mau,
        'mau' as is_mau,
        CASE
            WHEN f_mau_0 is false THEN 'no_mau' 
            WHEN f_mau_0 is true AND mes = mes_ob THEN '02. M0'
            WHEN f_mau_0 is true AND es_consecutivo is false THEN '01. Esporádico'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND LEAD(es_consecutivo,1) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN '03. M1'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND LEAD(es_consecutivo,2) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN '04. M2'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND LEAD(es_consecutivo,3) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN '05. M3'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND LEAD(es_consecutivo,4) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN '06. M4'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND LEAD(es_consecutivo,5) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN '07. M5'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND LEAD(es_consecutivo,6) OVER (PARTITION BY user ORDER BY mes DESC) = false  THEN '08. M6'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND previous_7 is true AND LEAD(es_consecutivo,7) OVER (PARTITION BY user ORDER BY mes DESC) = false  THEN '09. M7'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND previous_7 is true AND previous_8 is true AND LEAD(es_consecutivo,8) OVER (PARTITION BY user ORDER BY mes DESC) = false  THEN '10. M8'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND previous_7 is true AND previous_8 is true AND previous_9 is true AND LEAD(es_consecutivo,9) OVER (PARTITION BY user ORDER BY mes DESC) = false  THEN '10. M9'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND previous_7 is true AND previous_8 is true AND previous_9 is true AND previous_10 is true AND LEAD(es_consecutivo,10) OVER (PARTITION BY user ORDER BY mes DESC) = false  THEN '11. M10'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND previous_7 is true AND previous_8 is true AND previous_9 is true AND previous_10 is true AND previous_11 is true AND LEAD(es_consecutivo,11) OVER (PARTITION BY user ORDER BY mes DESC) = false  THEN '12. M11'
            WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND previous_7 is true AND previous_8 is true AND previous_9 is true AND previous_10 is true AND previous_11 is true AND previous_12 is true AND LEAD(es_consecutivo,11) OVER (PARTITION BY user ORDER BY mes DESC) = true  THEN '13. M12'
        ELSE 'mau_del_mes'
    END AS mau_type
    FROM mau_type
)
    SELECT
        *
    FROM arrg_data
