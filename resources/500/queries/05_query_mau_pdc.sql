---- DETALLE DE MAU PDC
DROP TABLE IF EXISTS `${project_target}.temp.query_mau_pdc`;
CREATE TABLE `${project_target}.temp.query_mau_pdc` AS (

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
    max(linea like '%utility_payments%') f_mau
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
    --CASE WHEN f_mau_0 is true THEN 'mau' ELSE 'no_mau' END AS is_mau,
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
    *
FROM arrg_data
ORDER BY 1 DESC

);


--- RESUMEN MAU PDC
DROP TABLE IF EXISTS `${project_target}.temp.query_mau_pdc_summary`;
CREATE TABLE `${project_target}.temp.query_mau_pdc_summary` AS (

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
    max(linea like '%utility_payments%') f_mau
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
    --CASE WHEN f_mau_0 is true THEN 'mau' ELSE 'no_mau' END AS is_mau,
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


--- APERTURA MAU Y DESCRIPTIVOS
DROP TABLE IF EXISTS `${project_target}.temp.query_mau_pdc_apertura`;
CREATE TABLE `${project_target}.temp.query_mau_pdc_apertura` AS (

with compras as (

    select
        date_trunc(A.fecha,month) mes,
        A.fecha,
        A.trx_timestamp,
        A.user,
        A.nombre, 
        A.trx_id,
        A.monto,
        case when date_trunc(A.fecha,month) >= tarjeta_fisica.mes then true else false end as tenencia_tf
    from ${project_source}.economics.economics A
    left join (

        SELECT
            fecha_activacion,
            date_trunc(fecha_activacion,MONTH) mes,
            user
        FROM `${project_source}.funnel.funnel_physical_card`
        where paso = 'Tarjeta activada'
        ORDER BY 1 desc
    ) tarjeta_fisica USING (user)
    where linea like '%utility_payments%'

)

select
    compras.mes,
    compras.user,
    mau_type.apertura_mau,
    IFNULL(saldo.saldo_promedio,0) saldo_promedio,
    count(trx_id) transacciones_totales,
    sum(monto) gpv_total,
    count(case when tenencia_tf and nombre like '%Nacional%' then trx_id else null end) transacciones_tarjeta_fisica_nacional,
    sum(case when tenencia_tf and nombre like '%Nacional%' then monto else 0 end) GPV_transacciones_tarjeta_fisica_nacional,
    count(case when tenencia_tf and nombre not like '%Nacional%' then trx_id else null end) transacciones_tarjeta_fisica_internacional,
    sum(case when tenencia_tf and nombre not like '%Nacional%' then monto else 0 end) GPV_transacciones_tarjeta_fisica_internacional,
    count(case when tenencia_tf is false and nombre like '%Nacional%' then trx_id else null end) transacciones_tarjeta_virtual_nacional,
    sum(case when tenencia_tf is false and nombre like '%Nacional%' then monto else 0 end) GPV_transacciones_tarjeta_virtual_nacional,
    count(case when tenencia_tf is false and nombre not like '%Nacional%' then trx_id else null end) transacciones_tarjeta_virtual_internacional,
    sum(case when tenencia_tf is false and nombre not like '%Nacional%' then monto else 0 end) GPV_transacciones_tarjeta_virtual_internacional,
from compras
join (

    select 
    *,
    case when mau_type in ('mau 1M','mau 2M','mau 3M','mau 4M','mau 5M') then 'MAU M1-M5' else mau_type end as apertura_mau
from `${project_target}.temp.query_mau_pdc` 

) mau_type on mau_type.user = compras.user and mau_type.mes = compras.mes

left join (

    WITH balance AS 
        (
        SELECT 
            date_trunc(fecha,MONTH) mes,
            *
        FROM `${project_source}.balance.daily_balance`
        )

    SELECT
        mes,
        user,
        avg(saldo_dia) saldo_promedio
    FROM balance
    group by 1,2
    order by user, mes desc

) saldo on saldo.user = compras.user and saldo.mes = compras.mes
group by 1,2,3,4
order by 2,1 desc


);

--- Descriptivos finales
SELECT 
    mes,
    apertura_mau,
    ROUND(avg(saldo_promedio)) saldo_promedio,
    ROUND(sum(transacciones_totales)) transacciones_totales,
    ROUND(sum(transacciones_tarjeta_fisica_nacional)) transacciones_tarjeta_fisica_nacional,
    ROUND(sum(transacciones_tarjeta_fisica_internacional)) transacciones_tarjeta_fisica_internacional,
    ROUND(sum(transacciones_tarjeta_virtual_nacional)) transacciones_tarjeta_virtual_nacional,
    ROUND(sum(transacciones_tarjeta_virtual_internacional)) transacciones_tarjeta_virtual_internacional,
    ROUND(sum(gpv_total)) gpv_total,
    ROUND(sum(GPV_transacciones_tarjeta_fisica_nacional)) GPV_transacciones_tarjeta_fisica_nacional,
    ROUND(sum(GPV_transacciones_tarjeta_fisica_internacional)) GPV_transacciones_tarjeta_fisica_internacional,
    ROUND(sum(GPV_transacciones_tarjeta_virtual_nacional)) GPV_transacciones_tarjeta_virtual_nacional,
    ROUND(sum(GPV_transacciones_tarjeta_virtual_internacional)) GPV_transacciones_tarjeta_virtual_internacional
FROM `${project_target}.temp.query_mau_pdc_apertura`
GROUP BY 1,2
ORDER BY 1 DESC,2 DESC
;

--- Descriptivos Numeros MAU TYPES
with data as (select
    *,
    case when mau_type in ('mau 1M','mau 2M','mau 3M','mau 4M','mau 5M') then 'MAU M1-M5' else mau_type end as apertura_mau
from `${project_target}.temp.query_mau_pdc_summary`
where mau_type <> 'no_mau'
order by mes desc
)

select 
    mes,
    users
from data
where apertura_mau = 'mau_mes_ob'
and mes <= '2022-03-01'
and mes >= '2021-01-01'
order by mes asc