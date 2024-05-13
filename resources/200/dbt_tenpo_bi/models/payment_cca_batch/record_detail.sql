{{ config(tags=["hourly", "bi"], materialized='table') }}

WITH data AS 

(SELECT
    user_id as user,
    created_at,
    updated_at,
    id,
    batch_header_id,
    prepaid_id,
    detail_reference_id,
    transaction_code,
    ifr,
    {{ hash_sensible_data('rut') }} as rut,
    {{ hash_sensible_data('name') }} as name,
    account_number,
    amount,
    optional_entry_code,
    addendum_indicator,
    trace_number,
    validation_error,
    status
FROM {{ source('payment_cca_batch', 'record_detail') }}

)

SELECT * FROM data