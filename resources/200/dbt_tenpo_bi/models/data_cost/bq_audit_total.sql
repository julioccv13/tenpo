{{ 
  config(
    tags=["daily", "bi"],
    enabled=True
  ) 
}}

    select 
        *
        ,'tenpo_airflow_prod' as proyecto
    from {{ref('bq_audit_tenpo_airflow_prod')}}
    union all 
    select 
        * 
        ,'tenpo_bi_prod' as proyecto
    from {{ref('bq_audit_tenpo_bi_prod')}}
    union all 
    select 
        * 
        ,'tenpo_bi' as proyecto
    from {{ref('bq_audit_tenpo_bi')}}
    union all 
    select 
        * 
        ,'tenpo_datalake_prod' as proyecto
    from {{ref('bq_audit_tenpo_datalake_prod')}}