{{ 
  config(
    materialized='ephemeral', 
  ) 
}}


SELECT 
  DISTINCT t.originSbifCode,
  COALESCE(bank.name, '___TENPO_APP___') AS banco_tienda_origen
FROM `tenpo-airflow-prod.payment_cca.payment_transaction`t 
LEFT JOIN `tenpo-airflow-prod.payment_cca.bank` bank 
ON CAST(t.originSbifCode AS INT64) = bank.sbif_code