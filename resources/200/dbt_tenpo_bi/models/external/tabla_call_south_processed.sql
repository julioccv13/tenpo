{{config(
    materialized='table',
    tags=["hourly"],
  ) 
}}

SELECT
  CAST(llamadas_recibidas AS INT) AS llamadas_recibidas,
  CAST(llamadas_atendidas AS INT) AS llamadas_atendidas,
  rut,
  opcion_del_menu,
  fecha_larga,
  ani,
  CAST(tmo as FLOAT64) as tmo,
  ejecutivo,
  respuesta_pregunta_1,
  respuesta_pregunta_2,
  definicion,
  execution_date
 FROM `tenpo-bi-prod.external.call_south`