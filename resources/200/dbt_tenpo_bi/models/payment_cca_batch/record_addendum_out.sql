{{ config(tags=["hourly", "bi"], materialized='table') }}

WITH data AS 

(SELECT
    A.created_at,
    A.updated_at,
    A.id,
    A.detail_out_id,
    A.register_code,
    A.addendum_code_type,
    A.return_code,
    A.original_trace_number,
    A.original_ifr,
    A.information_return,
    A.trace_number,
    A.settlement_date,
    A.original_return_code,
    A.return_trace_number,
FROM {{ source('payment_cca_batch', 'record_addendum_out') }} A

)

SELECT * FROM data