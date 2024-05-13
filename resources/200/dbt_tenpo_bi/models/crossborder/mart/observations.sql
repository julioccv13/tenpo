{{ 
  config(
    tags=["hourly", "crossborder", "datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT15', 'tenpo-datalake-sandbox')
  ) 
}}

SELECT * FROM `tenpo-airflow-prod.crossborder.observations`