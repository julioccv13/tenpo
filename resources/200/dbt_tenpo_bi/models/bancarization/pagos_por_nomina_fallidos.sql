{{ 
  config(
    materialized='table', 
  ) 
}}


SELECT 
    DISTINCT 
      A.*, 
      B.return_code,
      B.information_return,
      D.origin_company_name,
      D.ifo,
      COALESCE(E.banco_tienda_origen, '___TENPO_APP___') AS banco_tienda_origen,
FROM {{ ref('record_detail_out') }} A
JOIN {{ ref('record_addendum_out') }} B ON A.id = B.detail_out_id
JOIN {{ ref('record_detail') }} C on C.rut = A.rut
JOIN {{ ref('record_batch_header') }}  D ON C.batch_header_id = D.id
LEFT JOIN {{ ref('banks') }} E ON LEFT(D.ifo,4) = E.originSbifCode
