-- SELECT 1 as numero_1
DROP TABLE IF EXISTS `${project_target}.temp.query_tipo_mau`;
CREATE TABLE `${project_target}.temp.query_tipo_mau` AS (

WITH users as (
SELECT DISTINCT 
    id as user
      ,DATE_TRUNC(date(ob_completed_At, 'America/Santiago'), MONTH) as mes_ob
    FROM `${project_source}.users.users_tenpo`

),

economics AS 
(
SELECT
    DATE_TRUNC(fecha, MONTH) as mes,
    user,
    max(linea not in ('reward','saldo','pfm') and nombre not like '%Devol%') f_mau
FROM `${project_source}.economics.economics`
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

), mau_type AS

(SELECT
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
    
FROM data
ORDER BY 1 DESC),

arrg_data AS (

SELECT 
    *,
    'mau' as is_mau,
    CASE
        WHEN f_mau_0 is false THEN 'no_mau' 
        WHEN f_mau_0 is true AND mes = mes_ob THEN 'mau_mes_ob'
        WHEN f_mau_0 is true AND es_consecutivo is false THEN 'mau_del_mes'
        WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND LEAD(es_consecutivo,1) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN 'mau 1M'
        WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND LEAD(es_consecutivo,2) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN 'mau 2M'
        WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND LEAD(es_consecutivo,3) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN 'mau 3M'
        WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND LEAD(es_consecutivo,4) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN 'mau 4M'
        WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND LEAD(es_consecutivo,5) OVER (PARTITION BY user ORDER BY mes DESC) = false THEN 'mau 5M'
        WHEN f_mau_0 is true AND es_consecutivo is true AND previous_1 is true AND previous_2 is true AND previous_3 is true AND previous_4 is true AND previous_5 is true AND previous_6 is true AND LEAD(es_consecutivo,5) OVER (PARTITION BY user ORDER BY mes DESC) = true  THEN 'mau 6M'
        ELSE 'mau_del_mes'
    END AS mau_type
FROM mau_type

)

SELECT
    mes,
    mau_type,
    COUNT(*) users,
FROM arrg_data
GROUP BY 1,2 ORDER BY 1 DESC


);


