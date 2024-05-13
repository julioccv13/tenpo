{% snapshot economics_snapshot %}

{{
    config(
      target_schema='economics',
      unique_key='trx_id',

      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True
    )
}}


select * from {{ ref('economics') }}
{% endsnapshot %}