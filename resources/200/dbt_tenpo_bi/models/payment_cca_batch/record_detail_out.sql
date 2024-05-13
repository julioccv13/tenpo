{{ config(tags=["hourly", "bi"], materialized='table') }}

WITH data AS 

(SELECT
    A.created_at,
    A.updated_at,
    C.id as user,
    A.id,
    A.external_id,
    A.batch_header_out_id,
    A.detail_reference_id,
    A.prepaid_id,
    A.transaction_code,
    A.ifr,
    A.check_digit,
    {{ hash_sensible_data('A.rut') }} as rut,
    {{ hash_sensible_data('A.name') }} as name,
    A.account_number,
    A.amount,
    A.optional_entry_code,
    A.addendum_indicator,
    A.trace_number,
    A.status,
    A.validation_error,
FROM {{ source('payment_cca_batch', 'record_detail_out') }} A
JOIN {{ source('identity', 'ruts') }} B on RIGHT(A.rut,9) = REPLACE(B.rut_complete,'-','')
JOIN {{ source('tenpo_users', 'users') }} C USING (tributary_identifier)

)

SELECT * FROM data