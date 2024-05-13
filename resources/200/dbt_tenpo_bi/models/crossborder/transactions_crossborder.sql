{{ 
  config(
    tags=["hourly", "bi"],
    materialized='table', 
    cluster_by = "state",
    partition_by = {'field': 'date_trx_created_at', 'data_type': 'date'},
  ) 
}}

with users as (
  select * from {{source('crossborder','users')}}
  where true
  qualify row_number() over (partition by id order by updated_at desc) = 1
),

account as (
    select distinct * from {{source('crossborder','account')}}
    where true
  qualify row_number() over (partition by id order by updated_at desc) = 1  
),
users_tenpo as (
    select distinct * from {{source('crossborder','users')}}
  where true
  qualify row_number() over (partition by tenpo_id order by updated_at desc) = 1  
),

transactions as(
  select * from {{source('crossborder','transaction')}}
  where true
  qualify row_number() over (partition by id order by updated_at desc) = 1
),
users_snapshot as (
  select 
  distinct
  id,
  tenpo_id,
  dbt_valid_from,
  IF(dbt_valid_to IS NULL, CURRENT_TIMESTAMP(), dbt_valid_to) dbt_valid_to,
  case when status = 'OBSERVED' then 1 else 0 end OBSERVED_FLAG
  from {{ref('users_cb_snapshot')}}
  where status = 'OBSERVED'
)

select 
  distinct
  t.*,
  date(t.created_at) as date_trx_created_at,
  
  s.created_at as sender_created_at,
  s.country_code as sender_country,
  s.status as sender_status,

  u.created_at as receiver_created_at,
  u.country_code as receiver_country,
  u.status as receiver_status,
  
  a.account_type as receiver_account_type,
  a.bank_name as receiver_bank_name,
  
  if(t.state='SUCCESSFUL',TIMESTAMP_DIFF(t.updated_at, t.created_at, HOUR),null) as time_to_deliver,
  IF(if(t.state='SUCCESSFUL',TIMESTAMP_DIFF(t.updated_at, t.created_at, HOUR),null) > 72, "Fuera SLA", "Dentro SLA") as sla,
  IF(us.dbt_valid_from > t.created_at AND t.updated_at < us.dbt_valid_to AND us.OBSERVED_FLAG = 1, 1, 0) receiver_observed_flag,
  IF(su.dbt_valid_from > t.created_at AND t.updated_at < su.dbt_valid_to AND su.OBSERVED_FLAG = 1, 1, 0) sender_observed_flag,
  GREATEST(IF(us.dbt_valid_from > t.created_at AND t.updated_at < us.dbt_valid_to AND us.OBSERVED_FLAG = 1, 1, 0),IF(su.dbt_valid_from > t.created_at AND t.updated_at < su.dbt_valid_to AND su.OBSERVED_FLAG = 1, 1, 0)) observed_flag
from transactions t 
  left join users u on (t.receiver_user_id=u.id)
  left join users_tenpo s on (t.user_id=s.tenpo_id)
  left join account a on (t.destination_account_id=a.id)
  left join users_snapshot us on (t.receiver_user_id=us.id)
  left join users_snapshot su on (t.user_id=su.tenpo_id)
where true
qualify row_number() over (partition by id order by observed_flag desc) = 1