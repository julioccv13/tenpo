{{ config(tags=["hourly", "bi"], materialized='table') }}

WITH data AS 

(SELECT
    A.created_at,
    A.updated_at,
    A.id,
    A.file_id,
    A.service_class_code,
    A.origin_company_name,
    A.origin_account_number,
    A.origin_rut,
    A.standar_entry_class,
    A.description,
    A.descriptive_date,
    A.effective_date,
    A.settlement_date,
    A.status_ifo,
    A.ifo,
    A.batch_number,
FROM {{ source('payment_cca_batch', 'record_batch_header') }} A

)

SELECT * FROM data