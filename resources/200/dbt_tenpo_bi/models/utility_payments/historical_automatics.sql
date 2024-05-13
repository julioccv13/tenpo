{{ config(materialized='table') }}

SELECT * FROM {{source('tenpo_utility_payment','historical_automatics')}}
