{{ 
  config(
    materialized='table'
    ,tags=["daily", "bi"]
    ,grant_access_to=[
      {'project': 'tenpo-airflow-prod', 'dataset': 'transactions_history'},
    ]
  )
}}

select *
from {{source('transactions_history','transactions_history')}}