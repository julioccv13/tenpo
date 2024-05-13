{{ 
  config(
    materialized='table', 
    tags=["daily", "bi", "dracarys","datamart"],
    project=env_var('DBT_PROJECT8', 'tenpo-datalake-sandbox')
  ) 
}}

-- TODO: Deberiamos ver si es que hacemos carpetas o separamos en paquetes.
SELECT * FROM {{source('tenpo_users','due_diligence_results')}}