SELECT reporte FROM (
  SELECT DISTINCT SAFE_CONVERT_BYTES_TO_STRING(RPAD(CAST(reporte as bytes),78)) as reporte, rank_number FROM(
    SELECT
      CODIGO_INSTITUCION_FINANCIERA || 
      IDENTIFICACION_ARCHIVO || 
      REPLACE(SAFE_CAST(FECHA AS STRING), "-", ""
      ) AS reporte,
      2 as rank_number
    FROM  
      `${project_name}.${dataset}.D10_primer_registro`
    WHERE 
      EXECUTION_DATE = (SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.D10_primer_registro`)

  UNION ALL

    SELECT 
      (RUT ||
      NOMBRE ||
      TIPO_DEUDOR ||
      TIPO_CREDITO ||
      MOROSIDAD ||
      MONTO_INFORMADO) AS reporte,
      3 as rank_number
    FROM
      `${project_name}.${dataset}.D10_reporte`
    WHERE 
      EXECUTION_DATE = (SELECT MAX(EXECUTION_DATE) FROM `${project_name}.${dataset}.D10_reporte`)
  )
)
ORDER BY 
  rank_number