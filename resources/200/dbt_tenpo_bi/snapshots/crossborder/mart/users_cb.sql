{% snapshot users_cb_snapshot %}

{{
    config(
      target_schema='crossborder',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      project=env_var('DBT_PROJECT15', 'tenpo-datalake-sandbox')
    )
}}


select * from {{source('crossborder','users')}}
where TRUE
qualify ROW_NUMBER() over (partition by id order by updated_at) = 1

{% endsnapshot %}