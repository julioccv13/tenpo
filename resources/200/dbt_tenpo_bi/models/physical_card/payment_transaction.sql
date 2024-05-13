{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT DISTINCT
  t1.user_id,
  t1.amount,
  {{ hash_sensible_data('t1.email') }} as email,
  #t4.rut,
  cast(t5.id as INT64) as id_transaccion_prepago,
  t5.fecha_creacion as fecha_transaccion_prepago
FROM `tenpo-airflow-prod.tenpo_physical_card.payment_transaction` t1
  join `tenpo-airflow-prod.users.users` t4 on (t4.id=t1.user_id)
  join `tenpo-airflow-prod.prepago.prp_movimiento`  t5 on (t5.id_tx_externo = t1.id)

