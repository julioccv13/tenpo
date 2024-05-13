{{ config(materialized='table') }}

SELECT * FROM {{source('tenpo_topup','historical_automatics')}}
