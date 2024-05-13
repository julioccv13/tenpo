
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "linea",
    partition_by = {'field': 'fecha', 'data_type': 'date'},
  ) 
}}


SELECT
  * 
FROM {{ ref('economics') }}
WHERE
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), trx_timestamp, DAY) <= 365
   

