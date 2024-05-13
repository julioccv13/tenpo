{{ config(materialized='table') }}

SELECT 
    *
FROM {{source('clevertap_bucket','onfido')}}