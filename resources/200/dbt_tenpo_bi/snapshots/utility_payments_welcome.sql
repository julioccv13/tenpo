{% snapshot snapshots_tenpo_utility_payments_welcome %}

{{
    config(
      target_schema='snapshots',
      unique_key='id',

      strategy='check',
      check_cols=['tos','visits'],
    )
}}

select
distinct  
id
,last_value(user) over (partition by id order by created desc) as user
,max(created) over (partition by id) as created
,max(visits) over (partition by id) as visits
,max(tos) over (partition by id) as tos
 from {{ source('tenpo_utility_payment', 'welcome') }}

{% endsnapshot %}