{{ 
  config(
    materialized='table', 
  ) 
}}

SELECT
    DISTINCT
    A.created_at,
    A.updated_at,
    A.status,
    A.user,
    prepaid_id as trx_id,
    A.amount AS monto,
    B.origin_company_name,
    B.description,
    B.ifo as banco,
    COALESCE(C.banco_tienda_origen, '___TENPO_APP___') AS banco_tienda_origen,
    B.origin_account_number,    

FROM {{ ref('record_detail') }} A
JOIN {{ ref('record_batch_header') }}  B ON A.batch_header_id = B.id
LEFT JOIN {{ ref('banks') }} C ON LEFT(B.ifo,4) = C.originSbifCode
and user IS NOT NULL
ORDER BY 1 DESC
