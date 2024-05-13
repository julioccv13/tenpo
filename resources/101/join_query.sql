DROP TABLE IF EXISTS `${project_target}.temp.selyt_out_${ds_nodash}`;
CREATE TABLE `${project_target}.temp.selyt_out_${ds_nodash}` AS (

    WITH selyt_onboardings AS (
        SELECT 
            a.*
            ,id as user
            ,DATE(b.ob_completed_at, "America/Santiago") as fecha_registro_Tenpo
            ,coalesce(source, '') like '%IG%' as source_invita_y_gana
        FROM `${project_target}.temp.selyt_input_${ds}` a
        LEFT JOIN `${project3}.users.users` b
        ON lower(a.rut_cliente) = lower(b.rut)
    ), economics_minimum AS (
        SELECT 
            a.user,
            MIN(fecha) as fecha_primera_compra
        FROM selyt_onboardings A
        INNER JOIN `${project2}.economics.economics` B
        ON A.user = B.user
        WHERE linea IN ('mastercard', 'mastercard_physical', 'utility_payments', 'topups', 'paypal')
        GROUP BY 1
    )
    
    SELECT 
    a.idoportunidad
    ,FORMAT_DATE("%d-%m-%Y", DATE(a.fecha_oportunidad)) as fecha_oportunidad
    ,FORMAT_DATE("%d-%m-%Y", a.fecha_registro_Tenpo) as fecha_registro_Tenpo
    ,FORMAT_DATE("%d-%m-%Y", b.fecha_primera_compra) as fecha_primera_compra
    ,REPLACE(a.rut_cliente, '-', '') as rut_cliente
    ,a.nombre_cliente
    ,a.mail_cliente
    ,a.telefono_cliente
    ,a.source_invita_y_gana
    FROM selyt_onboardings a
    LEFT JOIN economics_minimum b
    ON a.user = b.user
    WHERE TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY a.mail_cliente ORDER BY a.fecha_registro_Tenpo, b.fecha_primera_compra ASC) = 1


);