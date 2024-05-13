{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

SELECT distinct
  id,
  user_id,
  case 
    when operation_code like '%R' then 'withdrawal'
    when operation_code like '%S' then 'investment'
    else null
    end as tipo_operacion,
  case 
    when (operation_code like '%R' and cast(paid as bool)) or (operation_code like '%S') then 'finished'
    when (operation_code like '%R' and paid is null) then 'pending'
    else null
    end as status,
  if (investment_type='QUOTES',cast(units as float64)*valor_cuota,cast(units as float64)) as monto,
  cast(cast(a.fund_code as float64) AS INTEGER) as fund_code,
  cast(cast(result_operation_id as float64) as int64) as result_operation_id,
  a.updated_at,
  date(timestamp_seconds(cast(cast(a.settlement_date as float64) AS INTEGER))) as settlement_date,
FROM {{source('tyba_payment','investments')}} a 
  left join {{ref('tyba_valor_cuota')}} b on (date(a.created_at) = b.FECHA and CAST(left(a.fund_code,4) AS INTEGER) = b.id_fondo)
  left join {{source('tyba_payment','deposit_conciliation')}} c on (CAST(cast(a.result_operation_id as float64) AS INTEGER) =cast(c.rd_id as INTEGER))
where status = 'APPROVED'
qualify row_number() over (partition by a.id order by a.updated_at desc) = 1