{% snapshot snapshots_pay_cliente %}
{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='check',
      check_cols=['fec_hora_actualizacion','fec_ultimo_intento_login','fec_ultimo_login_exitoso'],
      cluster_by = "id",
      partition_by={
        "field": "dbt_updated_at",
        "data_type": "timestamp",
      }
    )
}}

with data as (
  SELECT distinct
     *,
     ROW_NUMBER() OVER (PARTITION BY id ORDER BY fec_hora_actualizacion DESC) as row_no
  from {{source('paypal','pay_cliente')}}
)
-- Deduplicado
SELECT 
* EXCEPT (row_no)
FROM data
where row_no = 1

{% endsnapshot %}