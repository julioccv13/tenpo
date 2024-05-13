{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

select distinct
  t3.id id_orden_transaccion,
  LAST_VALUE(t3.status) OVER (PARTITION BY t3.id ORDER BY t3.updated ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_status,
  LAST_VALUE(t3.updated) OVER (PARTITION BY t3.id ORDER BY t3.updated ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_status_date,
  FROM `tenpo-airflow-prod.tenpo_physical_card.card_order` t3