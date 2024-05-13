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
                    (CODIGO_INSTITUCION_FINANCIERA || IDENTIFICACION_ARCHIVO || FECHA) AS reporte, 
                    1 as rank_number
                FROM 
                    `${project_name}.${dataset}.D91_primer_registro` 
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.D91_primer_registro`)
                
                UNION ALL

                SELECT
                    tipo_registro || fecha_operacion || Numero_identificacion_operacion || Monto_Operacion || Tasa_interes_Mensual || Plazo_Contractual || Cobros_adicionales_a_los_intereses || Numero_Cuotas || Monto_Cuotas || fecha_vencimiento_primera_cuota || fecha_vencimiento_ultima_cuota || tipo_operacion || Numero_Tarjeta as reporte,
                    2 as rank_number
                FROM
                    `${project_name}.${dataset}.D91_registro01`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.D91_registro01`)
                
                UNION ALL
                
                SELECT
                    tipo_registro || fecha_de_contratacion || fecha_operacion || Numero_identificacion_operacion || tipo_operacion || Monto_Autorizado_de_la_linea_de_credito || Monto_Operacion || Tasa_interes_Mensual || Plazo_Contractual || Comision as reporte,
                    5 as rank_number
                FROM
                    `${project_name}.${dataset}.D91_registro05`
                WHERE
                    EXECUTION_DATE = ( SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.D91_registro05`)
                
                
            )
    )
ORDER BY
    rank_number