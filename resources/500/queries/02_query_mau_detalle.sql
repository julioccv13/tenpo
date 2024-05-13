-- SELECT 1 as numero_2
DROP TABLE IF EXISTS `${project_target}.temp.query_mau_detalle`;
CREATE TABLE `${project_target}.temp.query_mau_detalle` AS (

WITH users as (
SELECT DISTINCT 
    id as user
      ,DATE_TRUNC(date(ob_completed_At, 'America/Santiago'), MONTH) as mes_ob
    FROM `${project_source}.users.users_tenpo`
    --where id = '15981331-ed2e-45d9-b3e6-99d82c58ed24'

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
    *
FROM arrg_data
ORDER BY 1 DESC

);


-- PERCENTILES DE TRX
DROP TABLE IF EXISTS `${project_target}.temp.product_type_percentiles`;
CREATE TABLE `${project_target}.temp.product_type_percentiles` AS (

WITH target AS 

    (SELECT
        DISTINCT user
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau 6M'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), 

    economics AS (

        SELECT 
            user,
            count(trx_id) as transacciones
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea <> 'reward' 
        AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
        AND nombre not like '%Devol%'
        AND linea <> 'saldo'
        AND linea <> 'pfm'
        GROUP BY 1

    ),array_data AS (
    SELECT
        target.user,
        economics.transacciones
    FROM target
    JOIN economics USING (user)
    ),

    percentiles AS 

        (SELECT
        percentil[offset(10)] as p10,
        percentil[offset(25)] as p25,
        percentil[offset(50)] as p50,
        percentil[offset(75)] as p75,
        percentil[offset(90)] as p90,
        from (

            select approx_quantiles(transacciones, 100) percentil from array_data

        )
    )

    SELECT * FROM percentiles

);



DROP TABLE IF EXISTS `${project_target}.temp.product_matriz`;
CREATE TABLE `${project_target}.temp.product_matriz` AS (


WITH target AS 

    (SELECT
        DISTINCT user
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau 6M'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT linea) as productos
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings')
        GROUP BY 1

    ), pivote AS

    (SELECT
        target.*,
        economics.productos,
        economics.transacciones,
        CASE
                WHEN transacciones <= 11 THEN 'bajo'
                WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
            ELSE 'alto' END as categoria
    FROM target
    JOIN economics USING (user)
    JOIN `${project_target}.temp.product_type_percentiles` B ON 1 = 1
    )

    SELECT * FROM
    (SELECT productos, user, categoria FROM pivote)
    PIVOT(COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
ORDER BY 1 ASC

);


DROP TABLE IF EXISTS `${project_target}.temp.rubros_matriz`;
CREATE TABLE `${project_target}.temp.rubros_matriz` AS (

WITH target AS 

    (SELECT
        DISTINCT 
            user,
            mau_type
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT rubro_recod) as rubros
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
        AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
        GROUP BY 1

    ), pivote AS

    (SELECT
        target.*,
        economics.rubros,
        economics.transacciones,
         CASE
                WHEN transacciones <= 11 THEN 'bajo'
                WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
            ELSE 'alto' END as categoria
    FROM target
    JOIN economics USING (user)
    JOIN `${project_target}.temp.product_type_percentiles` B ON 1 = 1
    )

    SELECT * FROM -- pivote
    (SELECT rubros, user,mau_type,categoria FROM pivote)
    PIVOT(COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
    ORDER BY 1 ASC



);






DROP TABLE IF EXISTS `${project_target}.temp.product_type_percentiles_mau_del_mes`;
CREATE TABLE `${project_target}.temp.product_type_percentiles_mau_del_mes` AS (

WITH target AS 

    (SELECT
        DISTINCT user
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau_del_mes'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), 

    economics AS (

        SELECT 
            user,
            count(trx_id) as transacciones
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
        AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
        GROUP BY 1

    ),array_data AS (
    SELECT
        target.user,
        economics.transacciones
    FROM target
    JOIN economics USING (user)
    ),

    percentiles AS 

        (SELECT
        percentil[offset(10)] as p10,
        percentil[offset(25)] as p25,
        percentil[offset(50)] as p50,
        percentil[offset(75)] as p75,
        percentil[offset(90)] as p90,
        from (

            select approx_quantiles(transacciones, 100) percentil from array_data

        )
    )

    SELECT * FROM percentiles

);




DROP TABLE IF EXISTS `${project_target}.temp.product_matriz_mau_del_mes`;
CREATE TABLE `${project_target}.temp.product_matriz_mau_del_mes` AS (


WITH target AS 

    (SELECT
        DISTINCT user
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau_del_mes'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT linea) as productos
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings')
        GROUP BY 1

    ), pivote AS

    (SELECT
        target.*,
        economics.productos,
        economics.transacciones,
        CASE
                WHEN transacciones <= 11 THEN 'bajo'
                WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
            ELSE 'alto' END as categoria
    FROM target
    JOIN economics USING (user)
    JOIN `${project_target}.temp.product_type_percentiles` B ON 1 = 1
    )

    SELECT * FROM
    (SELECT productos, user, categoria FROM pivote)
    PIVOT(COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
ORDER BY 1 ASC

);


DROP TABLE IF EXISTS `${project_target}.temp.rubros_matriz_mau_del_mes`;
CREATE TABLE `${project_target}.temp.rubros_matriz_mau_del_mes` AS (


WITH target AS 

    (SELECT
        DISTINCT user
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau_del_mes'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT rubro_recod) as rubros
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
        AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
        GROUP BY 1

    ), pivote AS

    (SELECT
        target.*,
        economics.rubros,
        economics.transacciones,
         CASE
                WHEN transacciones <= 11 THEN 'bajo'
                WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
            ELSE 'alto' END as categoria
    FROM target
    JOIN economics USING (user)
    JOIN `${project_target}.temp.product_type_percentiles` B ON 1 = 1
    )

    SELECT * FROM
    (SELECT rubros, user,categoria FROM pivote)
    PIVOT(COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
    ORDER BY 1 ASC

);



--- TOP PRODUCTOS
DROP TABLE IF EXISTS `${project_target}.temp.top_productos_matriz_mau_del_mes`;
CREATE TABLE `${project_target}.temp.top_productos_matriz_mau_del_mes` AS (

WITH target AS 

    (SELECT
        DISTINCT 
        user,
        mau_type
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT linea) as productos
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings')
        GROUP BY 1

    ), productos AS (

    
    SELECT 
            user,
            linea,
            count(*) trx
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings')
    GROUP BY 1,2 
    )
    
    ,pivote AS

    (SELECT
        target.*,
        economics.productos,
        economics.transacciones,
        CASE
                WHEN transacciones <= 11 THEN 'bajo'
                WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
            ELSE 'alto' END as categoria
    FROM target
    JOIN economics USING (user)
    JOIN `${project_target}.temp.product_type_percentiles` B ON 1 = 1
    )

    SELECT
        pivote.mau_type,
        pivote.categoria,
        linea producto,
        SUM(trx) transacciones,
        COUNT(DISTINCT pivote.user) usuarios_unicos
    FROM pivote
    JOIN productos USING (user)
    GROUP BY 1,2,3
    ORDER BY 1 DESC, 4 DESC

);

        DROP TABLE IF EXISTS `${project_target}.temp.top_productos_matriz_fix`;
        CREATE TABLE `${project_target}.temp.top_productos_matriz_fix` AS (
            WITH target AS 

            (SELECT
                DISTINCT 
                user,
                mau_type
            FROM `${project_target}.temp.query_mau_detalle`
            WHERE mes < DATE_TRUNC(current_date(), MONTH) 
            AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

            ), economics AS (

                SELECT 
                    user,
                    count(distinct trx_id) as transacciones,
                    CASE
                        when count(distinct trx_id) <= 11 THEN 'bajo'
                        when count(distinct trx_id) <= 24 AND count(distinct trx_id) > 11 THEN 'medio'
                    else 'alto' end as categoria
                FROM `${project_source}.economics.economics`
                WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
                AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
                AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
                AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
                GROUP BY 1
            ),
            productos AS (

                SELECT 
                    user,
                    linea,
                    nombre,
                    count(distinct case when linea in ('aum_savings','cash_in_savings','cash_out_savings') then 'bolsillo' when linea in ('aum_tyba','withdrawal_tyba','investment_tyba') then 'tyba' else linea end) over (partition by user) as productos,
                    count(*) trx_por_producto,
                    sum(monto) gpv
                FROM `${project_source}.economics.economics`
                WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
                AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
                AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
                AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
                GROUP BY 1,2,3 
            )
            select 
                target.user,
                target.mau_type,
                economics.categoria,
                economics.transacciones,
                productos.productos,
                productos.nombre,
                case when productos.linea in ('aum_savings','cash_in_savings','cash_out_savings') then 'bolsillo'
                    when productos.linea in ('aum_tyba','withdrawal_tyba','investment_tyba') then 'tyba'
                else productos.linea end as producto,
                productos.trx_por_producto,
                round(productos.gpv) gpv
            from target
            join economics using (user)
            join productos using (user)
            order by 1 desc
        );


        DROP TABLE IF EXISTS `${project_target}.temp.top_productos_matriz_fix_pivot`;
        CREATE TABLE `${project_target}.temp.top_productos_matriz_fix_pivot` AS (

        SELECT 
            *
        FROM (SELECT user , mau_type, productos , categoria FROM `${project_target}.temp.top_productos_matriz_fix`)
        PIVOT ( COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
        ORDER BY 1 ASC
        );


--- TOP RUBROS 
DROP TABLE IF EXISTS `${project_target}.temp.top_rubros_matriz_mau_del_mes`;
CREATE TABLE `${project_target}.temp.top_rubros_matriz_mau_del_mes` AS (

WITH target AS 

    (SELECT
        DISTINCT 
        user,
        mau_type
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

    ), economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT rubro_recod) as rubros
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
        GROUP BY 1

    ), rubros AS (

         SELECT 
            user,
            linea,
            rubro_recod,
            count(*) trx
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        and rubro_recod IS NOT NULL
        GROUP BY 1,2,3

    ),
    
    pivote AS

    (SELECT
        target.*,
        economics.rubros,
        economics.transacciones,
         CASE
                WHEN transacciones <= 11 THEN 'bajo'
                WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
            ELSE 'alto' END as categoria
    FROM target
    JOIN economics USING (user)
    JOIN `${project_target}.temp.product_type_percentiles` B ON 1 = 1
    )

SELECT 
    DISTINCT
        pivote.mau_type, 
        pivote.categoria,
        linea,
        rubro_recod,
        SUM(trx) transacciones,
        COUNT(DISTINCT pivote.user) usuarios_unicos
FROM pivote
JOIN rubros USING (user)
GROUP BY 1,2,3,4
ORDER BY 1 DESC, 4 DESC

);

    ----- #### CAMBIO RAPIDO ### -----
    DROP TABLE IF EXISTS `${project_target}.temp.top_rubros_matriz_mau_del_mes_fix`;
    CREATE TABLE `${project_target}.temp.top_rubros_matriz_mau_del_mes_fix` AS (

        WITH target AS 

        (SELECT
            DISTINCT 
            user,
            mau_type
        FROM `${project_target}.temp.query_mau_detalle`
        WHERE mes < DATE_TRUNC(current_date(), MONTH) 
        AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

        ), economics AS (

            SELECT 
                user,
                count(distinct trx_id) as transacciones,
                count(DISTINCT rubro_recod) as rubros,
                CASE
                    WHEN count(distinct trx_id) <= 11 THEN 'bajo'
                    WHEN count(distinct trx_id) <= 24 AND count(distinct trx_id) > 11 THEN 'medio'
                ELSE 'alto' END as categoria
            FROM `${project_source}.economics.economics`
            WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
            AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
            AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
            AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
            --and rubro_recod IS NOT NULL
            GROUP BY 1
        ),
        rubros AS (

            SELECT 
                user,
                linea,
                rubro_recod,
                count(*) trx_por_rubro,
                sum(monto) gpv
            FROM `${project_source}.economics.economics`
            WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
            AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
            and rubro_recod IS NOT NULL
            GROUP BY 1,2,3
        )
        select 
            target.user,
            target.mau_type,
            economics.categoria,
            economics.transacciones,
            economics.rubros,
            rubros.linea,
            rubros.rubro_recod,
            rubros.trx_por_rubro,
            round(rubros.gpv) gpv
        from target
        left join economics using (user)
        left join rubros using (user)
        order by 1 desc
    );

    DROP TABLE IF EXISTS `${project_target}.temp.top_rubros_matriz_fix_pivot`;
        CREATE TABLE `${project_target}.temp.top_rubros_matriz_fix_pivot` AS (

        SELECT 
            *
        FROM (SELECT user , mau_type, rubros , categoria FROM `${project_target}.temp.top_rubros_matriz_mau_del_mes_fix`)
        PIVOT ( COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
        ORDER BY 1 ASC
    );

    DROP TABLE IF EXISTS `${project_target}.temp.top_rubros_matriz_fix_pivot_4M_5M`;
    CREATE TABLE `${project_target}.temp.top_rubros_matriz_fix_pivot_4M_5M`AS (

        SELECT 
        *
        FROM (SELECT DISTINCT user,rubros,categoria FROM `${project_target}.temp.top_rubros_matriz_mau_del_mes_fix` where mau_type in ('mau 5M','mau 4M'))
        PIVOT ( COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
        ORDER BY 1 ASC
    );



--- SUB CATEGORIA ESPORADICO (MAU DEL MES) TOP PRODUCTOS

DROP TABLE IF EXISTS `${project_target}.temp.top_subcategoria_mau_top_productos`;
CREATE TABLE `${project_target}.temp.top_subcategoria_mau_top_productos` AS (

    WITH users AS (

        SELECT 
            * 
        FROM `${project_target}.temp.query_mau_detalle`
        WHERE mau_type = 'mau_del_mes'
        AND mes < DATE_TRUNC(current_date(), MONTH) 
        AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
    )

    , category AS 

    (SELECT
        user,
        case when count(*) < 4 then 'esporadico bajo' else 'esporadico alto' end as sub_category
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau_del_mes'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
    GROUP BY 1), 

    target AS 
    
    (SELECT  
        distinct 
        user
        ,sub_category
    FROM users
    JOIN category USING (user)
    ),
    economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            case
                    when count(distinct trx_id) <= 11 THEN 'bajo'
                    when count(distinct trx_id) <= 24 AND count(distinct trx_id) > 11 THEN 'medio'
            else 'alto' end as categoria
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
        AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
        AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
        GROUP BY 1

    ), productos AS (

         SELECT 
            user,
            linea,
            count(distinct case when linea in ('aum_savings','cash_in_savings','cash_out_savings') then 'bolsillo' when linea in ('aum_tyba','withdrawal_tyba','investment_tyba') then 'tyba' else linea end) over (partition by user) as productos,
            count(*) trx_por_producto,
            sum(monto) gpv
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_out_savings','withdrawal_tyba','investment_tyba')
        AND nombre not like '%Devol%'
    GROUP BY 1,2 

    )
    
    SELECT 
        target.user,
        target.sub_category as sub_category_mau,
        economics.categoria,
        productos.productos,
        case when productos.linea in ('aum_savings','cash_in_savings','cash_out_savings') then 'bolsillo'
                when productos.linea in ('aum_tyba','withdrawal_tyba','investment_tyba') then 'tyba'
        else productos.linea end as producto,
        economics.transacciones,
        productos.trx_por_producto,
        round(productos.gpv) gpv
    FROM target
    JOIN economics USING (user)
    JOIN productos USING (user)


);



--- SUB CATEGORIA ESPORADICO (MAU DEL MES) TOP RUBROS
DROP TABLE IF EXISTS `${project_target}.temp.top_subcategoria_mau_top_rubros`;
CREATE TABLE `${project_target}.temp.top_subcategoria_mau_top_rubros` AS (

WITH users AS (

        SELECT 
            * 
        FROM `${project_target}.temp.query_mau_detalle`
        WHERE mau_type = 'mau_del_mes'
        AND mes < DATE_TRUNC(current_date(), MONTH) 
        AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
    )

    , category AS 

    (SELECT
        user,
        case when count(*) < 4 then 'esporadico bajo' else 'esporadico alto' end as sub_category
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mau_type = 'mau_del_mes'
    AND mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
    GROUP BY 1), 

    target AS 
    
    (SELECT  
        distinct 
        user
        ,sub_category
    FROM users
    JOIN category USING (user)
    ),
    economics AS (

        SELECT 
            user,
            count(distinct trx_id) as transacciones,
            count(DISTINCT rubro_recod) as rubros,
            case
                    when count(distinct trx_id) <= 11 THEN 'bajo'
                    when count(distinct trx_id) <= 24 AND count(distinct trx_id) > 11 THEN 'medio'
            else 'alto' end as categoria
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
        AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
        AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
        GROUP BY 1

    ), rubros AS (

         SELECT 
                user,
                linea,
                rubro_recod,
                count(*) trx_por_rubro
            FROM `${project_source}.economics.economics`
            WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
            AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
            and rubro_recod IS NOT NULL
            GROUP BY 1,2,3

    )
    
    SELECT 
        target.user,
        target.sub_category as sub_category_mau,
        economics.categoria,
        economics.transacciones,
        economics.rubros,
        rubros.linea,
        rubros.rubro_recod,
        rubros.trx_por_rubro

    
    FROM target
    LEFT JOIN economics USING (user)
    LEFT JOIN rubros USING (user)
);



