{{ config(materialized='table') }}

SELECT * FROM {{source('tenpo_topup','automatics')}}
