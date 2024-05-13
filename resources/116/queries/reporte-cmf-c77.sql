SELECT
    reporte
FROM
(
        SELECT
            DISTINCT SAFE_CONVERT_BYTES_TO_STRING(RPAD(CAST(reporte as bytes), 34)) as reporte,
            rank_number
        FROM
            (
                SELECT 
                    (CODIGO_INSTITUCION_FINANCIERA || IDENTIFICACION_ARCHIVO || FECHA) AS reporte, 1 as rank_number
                FROM 
                    `${project_name}.${dataset}.C77_primer_registro` 
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.C77_primer_registro`)

                UNION ALL

                SELECT
                    Tipo_Reg || Destinatario_pago || Tipo_afiliado || Tipo_tarjeta || Plazo_pagos || Plazo_total_pagos || Monto_de_los_Pagos || Numero_de_Pagos as reporte,
                    2 as rank_number
                FROM
                    `${project_name}.${dataset}.C77_prepago`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.C77_prepago`)
                UNION
                ALL
                SELECT
                    Tipo_Reg || Destinatario_pago || Tipo_afiliado || Tipo_tarjeta || Plazo_pagos || Plazo_total_pagos || Monto_de_los_Pagos || Numero_de_Pagos as reporte,
                    3 as rank_number
                FROM
                    `${project_name}.${dataset}.C77_credito`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.C77_credito`)
                
                
            )
    )
ORDER BY
    rank_number