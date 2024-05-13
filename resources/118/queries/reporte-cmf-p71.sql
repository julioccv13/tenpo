SELECT
    reporte
FROM
(
        SELECT
            DISTINCT SAFE_CONVERT_BYTES_TO_STRING(RPAD(CAST(reporte as bytes), 126)) as reporte,
            rank_number
        FROM
            (
                SELECT 
                    (CODIGO_INSTITUCION_FINANCIERA || IDENTIFICACION_ARCHIVO || FECHA) as reporte, 
                    1 as rank_number
                FROM 
                    `${project_name}.${dataset}.P71_primer_registro`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.P71_primer_registro`)

                UNION ALL

                SELECT
                    (tipo_registro || tipo_tarjeta || marca_tarjeta || tipo_contrato || cantidad_de_contratos || cantidad_de_tarjetas_titulares || cantidad_de_tarjetas_adicionales) as reporte,
                    2 as rank_number
                FROM
                    `${project_name}.${dataset}.P71_credito_registro_01`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.P71_credito_registro_01`)
                UNION
                ALL
                SELECT
                    (tipo_registro || tipo_tarjeta || marca_tarjeta || tipo_contrato || cantidad_de_contratos || cantidad_de_tarjetas_vigentes || cantidad_de_tarjetas_con_movimiento || cantidad_de_contrats_no_vigentes_con_deuda) as reporte,
                    3 as rank_number
                FROM
                    `${project_name}.${dataset}.P71_credito_registro_02`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.P71_credito_registro_02`)
                UNION
                ALL
                SELECT
                    (tipo_registro || tipo_relacion_comercio || tipo_tarjeta || marca_tarjeta || tipo_contrato || tipo_operacion || cantidad_de_compras || monto_de_compras) as reporte,
                    4 as rank_number
                FROM
                    `${project_name}.${dataset}.P71_credito_registro_03_compras`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.P71_credito_registro_03_compras`)
                UNION
                ALL
                SELECT
                    (tipo_registro || tipo_relacion_comercio || tipo_tarjeta || marca_tarjeta || tipo_contrato || tipo_operacion || cantidad_de_pagos || monto_de_pagos) as reporte,
                    5 as rank_number
                FROM
                    `${project_name}.${dataset}.P71_credito_registro_03_pagos`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.P71_credito_registro_03_pagos`)
                UNION
                ALL
                SELECT
                    (tipo_registro || marca_tarjeta || tipo_contrato || tipo_tramo_morosidad || cantidad || Monto) as reporte,
                    6 as rank_number
                FROM
                    `${project_name}.${dataset}.P71_credito_registro_04`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.P71_credito_registro_04`)
            )
    )
ORDER BY
    rank_number