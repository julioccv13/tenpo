{% snapshot users_economics_snapshot %}

{{
    config(
      target_schema='users',
      unique_key='id',

      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True
    )
}}


select * from {{ ref('users_tenpo') }}
where TRUE
qualify ROW_NUMBER() over (partition by id order by updated_at DESC) = 1

{% endsnapshot %}