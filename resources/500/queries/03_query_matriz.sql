-- SELECT 1 AS numbero_3

DROP TABLE IF EXISTS `${project_target}.temp.product_matriz_cico`;
CREATE TABLE `${project_target}.temp.product_matriz_cico` AS (


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
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
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



DROP TABLE IF EXISTS `${project_target}.temp.product_matriz_mau_del_mes_cico`;
CREATE TABLE `${project_target}.temp.product_matriz_mau_del_mes_cico` AS (


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
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
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



--- TOP productos
DROP TABLE IF EXISTS `${project_target}.temp.top_productos_matriz_mau_del_mes_cico`;
CREATE TABLE `${project_target}.temp.top_productos_matriz_mau_del_mes_cico` AS (

WITH target AS 

    (SELECT
        DISTINCT 
        user,
        mau_type
    FROM `${project_target}.temp.query_mau_detalle`
    --WHERE mau_type = 'mau 6M'
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
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
        GROUP BY 1

    ), productos AS (

    
    SELECT 
            user,
            linea,
            count(*) trx
        FROM `${project_source}.economics.economics`
        WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
        AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
        AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
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



--- RUBRO SUSCRIPCIONES 
--- SUB CATEGORIA ESPORADICO (MAU DEL MES) TOP RUBROS
DROP TABLE IF EXISTS `${project_target}.temp.categoria_subscripsciones`;
CREATE TABLE `${project_target}.temp.categoria_subscripsciones` AS (

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
            GROUP BY 1
        ),
        rubros AS (

            SELECT 
                user,
                linea,
                rubro_recod,
                comercio_recod,
                comercio
            FROM `${project_source}.economics.economics`
            WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
            AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
            AND rubro_recod = 'Suscripciones'
            
        )
        select 
            target.user,
            target.mau_type,
            economics.categoria,
            economics.transacciones,
            economics.rubros,
            rubros.linea,
            rubros.rubro_recod,
            rubros.comercio_recod,
            rubros.comercio
        from target
        join economics using (user)
        join rubros using (user)
        order by 1 desc

);


DROP TABLE IF EXISTS `${project_target}.temp.descriptivos_adicionales`;
CREATE TABLE `${project_target}.temp.descriptivos_adicionales` AS (


WITH target AS (SELECT
            DISTINCT 
            user,
            mau_type
    FROM `${project_target}.temp.query_mau_detalle`
    WHERE mes < DATE_TRUNC(current_date(), MONTH) 
    AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
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

    ),

    saldo_promedio AS (

    SELECT 
        user,
        AVG(saldo_app) saldo_promedio
    FROM `${project_source}.balance.saldo_app` 
    WHERE DATE_TRUNC(DATE(fecha),MONTH) < DATE_TRUNC(current_date(), MONTH) 
    AND DATE_TRUNC(DATE(fecha),MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
    GROUP BY 1
    ), tarjeta_fisica AS (

    SELECT 
        distinct user
    FROM `${project_source}.funnel.funnel_physical_card`
    WHERE paso = 'Tarjeta activada' 

    )

    SELECT  
            DISTINCT
            mau_type,
            target.user,
            economics.categoria,
            ROUND(saldo_promedio.saldo_promedio) saldo_promedio,
            CASE WHEN tarjeta_fisica.user IS NULL then false ELSE true END AS tiene_tarjeta_fisica
    FROM target
    JOIN saldo_promedio USING (user)
    LEFT JOIN economics USING (user)
    LEFT JOIN tarjeta_fisica USING (user)

);


-------- TODOS LOS RUBROS DE 6 M --------

DROP TABLE IF EXISTS `${project_target}.temp.top_rubros_matriz_rubros_history`;
CREATE TABLE `${project_target}.temp.top_rubros_matriz_rubros_history` AS (

    WITH target AS 

        (SELECT
            DISTINCT 
                user,
                mau_type
        FROM `${project_target}.temp.query_mau_detalle`
        --WHERE mau_type = 'mau 6M'
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
        ),

        rubros AS (

            SELECT
                distinct
                    user,
                    rubro_recod
            FROM `${project_source}.economics.economics`
            --where rubro_recod is not null

        ), data AS


        (SELECT
            pivote.user,
            pivote.mau_type,
            pivote.categoria,
            rubros.rubro_recod
        FROM pivote
        JOIN rubros USING (user)
        )


    SELECT 
    distinct 
        user,
        rubro_recod,
        categoria,
        mau_type,
    FROM data
    --group by 1,2,3

);


-- DROP TABLE IF EXISTS `tenpo-datalake-sandbox.temp.product_matriz_cico`;
-- CREATE TABLE `tenpo-datalake-sandbox.temp.product_matriz_cico` AS (


-- WITH target AS 

--     (SELECT
--         DISTINCT user
--     FROM `tenpo-datalake-sandbox.temp.query_mau_detalle`
--     WHERE mau_type = 'mau 6M'
--     AND mes < DATE_TRUNC(current_date(), MONTH) 
--     AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

--     ), economics AS (

--         SELECT 
--             user,
--             count(distinct trx_id) as transacciones,
--             count(DISTINCT linea) as productos
--         FROM `tenpo-bi-prod.economics.economics`
--         WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--         AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--         AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
--         GROUP BY 1

--     ), pivote AS

--     (SELECT
--         target.*,
--         economics.productos,
--         economics.transacciones,
--         CASE
--                 WHEN transacciones <= 11 THEN 'bajo'
--                 WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
--             ELSE 'alto' END as categoria
--     FROM target
--     JOIN economics USING (user)
--     JOIN `tenpo-datalake-sandbox.temp.product_type_percentiles` B ON 1 = 1
--     )

--     SELECT * FROM
--     (SELECT productos, user, categoria FROM pivote)
--     PIVOT(COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
-- ORDER BY 1 ASC

-- );



-- DROP TABLE IF EXISTS `tenpo-datalake-sandbox.temp.product_matriz_mau_del_mes_cico`;
-- CREATE TABLE `tenpo-datalake-sandbox.temp.product_matriz_mau_del_mes_cico` AS (


-- WITH target AS 

--     (SELECT
--         DISTINCT user
--     FROM `tenpo-datalake-sandbox.temp.query_mau_detalle`
--     WHERE mau_type = 'mau_del_mes'
--     AND mes < DATE_TRUNC(current_date(), MONTH) 
--     AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

--     ), economics AS (

--         SELECT 
--             user,
--             count(distinct trx_id) as transacciones,
--             count(DISTINCT linea) as productos
--         FROM `tenpo-bi-prod.economics.economics`
--         WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--         AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--         AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
--         GROUP BY 1

--     ), pivote AS

--     (SELECT
--         target.*,
--         economics.productos,
--         economics.transacciones,
--         CASE
--                 WHEN transacciones <= 11 THEN 'bajo'
--                 WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
--             ELSE 'alto' END as categoria
--     FROM target
--     JOIN economics USING (user)
--     JOIN `tenpo-datalake-sandbox.temp.product_type_percentiles` B ON 1 = 1
--     )

--     SELECT * FROM
--     (SELECT productos, user, categoria FROM pivote)
--     PIVOT(COUNT(DISTINCT user) FOR categoria IN ('bajo','medio','alto'))
-- ORDER BY 1 ASC

-- );



-- --- TOP productos
-- DROP TABLE IF EXISTS `tenpo-datalake-sandbox.temp.top_productos_matriz_mau_del_mes_cico`;
-- CREATE TABLE `tenpo-datalake-sandbox.temp.top_productos_matriz_mau_del_mes_cico` AS (

-- WITH target AS 

--     (SELECT
--         DISTINCT 
--         user,
--         mau_type
--     FROM `tenpo-datalake-sandbox.temp.query_mau_detalle`
--     --WHERE mau_type = 'mau 6M'
--     WHERE mes < DATE_TRUNC(current_date(), MONTH) 
--     AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

--     ), economics AS (

--         SELECT 
--             user,
--             count(distinct trx_id) as transacciones,
--             count(DISTINCT linea) as productos
--         FROM `tenpo-bi-prod.economics.economics`
--         WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--         AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--         AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
--         GROUP BY 1

--     ), productos AS (

    
--     SELECT 
--             user,
--             linea,
--             count(*) trx
--         FROM `tenpo-bi-prod.economics.economics`
--         WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--         AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--         AND linea IN ('mastercard_physical','utility_payments','paypal','mastercard','investment_tyba','top_ups','p2p','crossborder','cash_in_savings','cash_in','cash_out')
--     GROUP BY 1,2 
--     )
    
--     ,pivote AS

--     (SELECT
--         target.*,
--         economics.productos,
--         economics.transacciones,
--         CASE
--                 WHEN transacciones <= 11 THEN 'bajo'
--                 WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
--             ELSE 'alto' END as categoria
--     FROM target
--     JOIN economics USING (user)
--     JOIN `tenpo-datalake-sandbox.temp.product_type_percentiles` B ON 1 = 1
--     )

--     SELECT
--         pivote.mau_type,
--         pivote.categoria,
--         linea producto,
--         SUM(trx) transacciones,
--         COUNT(DISTINCT pivote.user) usuarios_unicos
--     FROM pivote
--     JOIN productos USING (user)
--     GROUP BY 1,2,3
--     ORDER BY 1 DESC, 4 DESC

-- );



-- --- RUBRO SUSCRIPCIONES 
-- --- SUB CATEGORIA ESPORADICO (MAU DEL MES) TOP RUBROS
-- DROP TABLE IF EXISTS `tenpo-datalake-sandbox.temp.categoria_subscripsciones`;
-- CREATE TABLE `tenpo-datalake-sandbox.temp.categoria_subscripsciones` AS (

--     WITH target AS 

--         (SELECT
--             DISTINCT 
--             user,
--             mau_type
--         FROM `tenpo-datalake-sandbox.temp.query_mau_detalle`
--         WHERE mes < DATE_TRUNC(current_date(), MONTH) 
--         AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

--         ), economics AS (

--             SELECT 
--                 user,
--                 count(distinct trx_id) as transacciones,
--                 count(DISTINCT rubro_recod) as rubros,
--                 CASE
--                     WHEN count(distinct trx_id) <= 11 THEN 'bajo'
--                     WHEN count(distinct trx_id) <= 24 AND count(distinct trx_id) > 11 THEN 'medio'
--                 ELSE 'alto' END as categoria
--             FROM `tenpo-bi-prod.economics.economics`
--             WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--             AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--             AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
--             AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
--             GROUP BY 1
--         ),
--         rubros AS (

--             SELECT 
--                 user,
--                 linea,
--                 rubro_recod,
--                 comercio_recod,
--                 comercio
--             FROM `tenpo-bi-prod.economics.economics`
--             WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--             AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--             AND rubro_recod = 'Suscripciones'
            
--         )
--         select 
--             target.user,
--             target.mau_type,
--             economics.categoria,
--             economics.transacciones,
--             economics.rubros,
--             rubros.linea,
--             rubros.rubro_recod,
--             rubros.comercio_recod,
--             rubros.comercio
--         from target
--         join economics using (user)
--         join rubros using (user)
--         order by 1 desc

-- );


-- DROP TABLE IF EXISTS `tenpo-datalake-sandbox.temp.descriptivos_adicionales`;
-- CREATE TABLE `tenpo-datalake-sandbox.temp.descriptivos_adicionales` AS (


-- WITH target AS (SELECT
--             DISTINCT 
--             user,
--             mau_type
--     FROM `tenpo-datalake-sandbox.temp.query_mau_detalle`
--     WHERE mes < DATE_TRUNC(current_date(), MONTH) 
--     AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--     ),

--     economics AS (

--         SELECT 
--             user,
--             count(distinct trx_id) as transacciones,
--             case
--                     when count(distinct trx_id) <= 11 THEN 'bajo'
--                     when count(distinct trx_id) <= 24 AND count(distinct trx_id) > 11 THEN 'medio'
--             else 'alto' end as categoria
--         FROM `tenpo-bi-prod.economics.economics`
--         WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--         AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 6 MONTH)
--         AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
--         AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
--         GROUP BY 1

--     ),

--     saldo_promedio AS (

--     SELECT 
--         user,
--         AVG(saldo_app) saldo_promedio
--     FROM `tenpo-bi-prod.balance.saldo_app` 
--     WHERE DATE_TRUNC(DATE(fecha),MONTH) < DATE_TRUNC(current_date(), MONTH) 
--     AND DATE_TRUNC(DATE(fecha),MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--     GROUP BY 1
--     ), tarjeta_fisica AS (

--     SELECT 
--         distinct user
--     FROM `tenpo-bi-prod.funnel.funnel_physical_card`
--     WHERE paso = 'Tarjeta activada' 

--     )

--     SELECT  
--             DISTINCT
--             mau_type,
--             target.user,
--             economics.categoria,
--             ROUND(saldo_promedio.saldo_promedio) saldo_promedio,
--             CASE WHEN tarjeta_fisica.user IS NULL then false ELSE true END AS tiene_tarjeta_fisica
--     FROM target
--     JOIN saldo_promedio USING (user)
--     LEFT JOIN economics USING (user)
--     LEFT JOIN tarjeta_fisica USING (user)

-- );


-- -------- TODOS LOS RUBROS DE 6 M --------

-- DROP TABLE IF EXISTS `tenpo-datalake-sandbox.temp.top_rubros_matriz_rubros_history`;
-- CREATE TABLE `tenpo-datalake-sandbox.temp.top_rubros_matriz_rubros_history` AS (

--     WITH target AS 

--         (SELECT
--             DISTINCT 
--                 user,
--                 mau_type
--         FROM `tenpo-datalake-sandbox.temp.query_mau_detalle`
--         --WHERE mau_type = 'mau 6M'
--         WHERE mes < DATE_TRUNC(current_date(), MONTH) 
--         AND mes >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)

--         ), economics AS (

--             SELECT 
--                 user,
--                 count(distinct trx_id) as transacciones,
--                 count(DISTINCT rubro_recod) as rubros
--             FROM `tenpo-bi-prod.economics.economics`
--             WHERE DATE_TRUNC(fecha, MONTH) < DATE_TRUNC(current_date(), MONTH) 
--             AND DATE_TRUNC(fecha, MONTH) >= DATE_SUB(DATE_TRUNC(current_date(), MONTH), INTERVAL 3 MONTH)
--             AND linea <> 'reward' and nombre not like '%Devol%' and linea <> 'saldo' and linea <> 'pfm'
--             AND linea not in ('cash_in','cash_out','aum_tyba','aum_savings')
--             GROUP BY 1

--         ), pivote AS

--         (SELECT
--             target.*,
--             economics.rubros,
--             economics.transacciones,
--             CASE
--                     WHEN transacciones <= 11 THEN 'bajo'
--                     WHEN transacciones <= 24 AND transacciones > 11 THEN 'medio'
--                 ELSE 'alto' END as categoria
--         FROM target
--         JOIN economics USING (user)
--         JOIN `tenpo-datalake-sandbox.temp.product_type_percentiles` B ON 1 = 1
--         ),

--         rubros AS (

--             SELECT
--                 distinct
--                     user,
--                     rubro_recod
--             FROM `tenpo-bi-prod.economics.economics`
--             --where rubro_recod is not null

--         ), data AS


--         (SELECT
--             pivote.user,
--             pivote.mau_type,
--             pivote.categoria,
--             rubros.rubro_recod
--         FROM pivote
--         JOIN rubros USING (user)
--         )


--     SELECT 
--     distinct 
--         user,
--         rubro_recod,
--         categoria,
--         mau_type,
--     FROM data
--     --group by 1,2,3

-- );
