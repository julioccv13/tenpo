
{{ config(tags=["daily", "bi"], materialized='ephemeral') }}

select 
    fecha_carga,
    contrato,
    numero_documento
from {{ source('reconciliation', 'trx_ident_contrato') }}
qualify row_number() over (partition by numero_documento order by fecha_carga desc) = 1

