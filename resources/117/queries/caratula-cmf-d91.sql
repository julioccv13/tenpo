SELECT DISTINCT CAST(Monto AS INT64) AS Cuenta , Tipo, orden, EXECUTION_DATE FROM `${project_name}.${dataset}.${codigo_reporte}_caratula` WHERE EXECUTION_DATE = (SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.${codigo_reporte}_caratula`) ORDER BY orden
