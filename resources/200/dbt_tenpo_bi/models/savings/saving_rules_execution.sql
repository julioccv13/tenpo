{{ 
  config(
    tags=["daily", "bi"],
    materialized='table'
  ) 
}}

select 
    re.* except(created_at,updated_at,id,amount,rule_type),
    re.created_at as re_created_at,
    re.updated_at as re_updated_at,
    re.id as re_id,
    re.amount as re_amount,
    re.rule_type as re_rule_type,
    r.* except(created_at,updated_at,id,amount,rule_type),
    r.rule_type as r_rule_type,
    r.created_at as r_created_at,
    r.updated_at as r_updated_at,
    r.id as r_id,
    r.amount as r_amount,
  FROM {{source('savings_rules','rule_execution_history')}} re 
    join {{ref('saving_rules_created')}} AS r on (re.rule_id = r.id)
    join {{ source('payment_savings', 'cash_in') }} ci on (ci.cash_in_id=re.cash_in_id)
    JOIN {{ ref('users_savings') }} u  ON ci.user_id = u.id
where TRUE
  AND ci.status in ( 'SUCCESSFUL', 'PENDING', 'CONFIRMED', 'CREATED')
  AND u.register_status in ('FINISHED')
  AND u.onboarding_status in ('FINISHED')
  AND u.origin = 'SAVINGS'
qualify ROW_NUMBER() over (partition by re.id order by re.updated_at)=1