{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH 
 users_savings as (
    select 
      * except (created_at,onboarding_date), 
      if (origin=source,created_at,created_at_user_onboarding)  
       as created_at,
      if (origin=source,onboarding_date,created_at_user_onboarding)  
       as onboarding_date
    from {{ref('users_savings')}} where origin = 'TYBA'

 ),
 inicio_ob as (
    SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Inicia OB' paso,
      0 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in ('START', 'NACIONALITY', 'WORK_STATUS', 'US_PERSON', 'TAX_RESIDENCE','EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED', 'VALIDATION_TYPE')

      ),
  
  inicio_ob_validacion_identidad as (
    SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Valida Identidad' paso,
      1 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in ('START', 'NACIONALITY', 'WORK_STATUS', 'US_PERSON', 'TAX_RESIDENCE','EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED', 'VALIDATION_TYPE')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
 ingresa_nacionalidad as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa nacionalidad' paso,
      2 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in ('NACIONALITY', 'WORK_STATUS', 'US_PERSON', 'TAX_RESIDENCE','EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
 ingresa_status_trabajo as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa status trabajo' paso,
      3 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in ( 'WORK_STATUS', 'US_PERSON', 'TAX_RESIDENCE','EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
 ingresa_us_person as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa us person' paso,
      4 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in (  'US_PERSON', 'TAX_RESIDENCE','EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
  ingresa_tax_residence as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa tax residence' paso,
      5 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in ( 'TAX_RESIDENCE','EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
  
  ingresa_pep as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa pep' paso,
      6 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in ( 'EXPOSED_PERSON',   'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
  ingresa_origen_fondos as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa origen fondos' paso,
      7 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in (  'FUND_ORIGIN',  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
  ingresa_cp_related as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa cp related' paso,
      8 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in (  'CP_RELATED', 'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
  ingresa_tyc as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(created_at, "America/Santiago") as fecha_funnel,
      'Ingresa t&c' paso,
      9 num,
      0 monto
    FROM users_savings
    WHERE 
      register_status in (  'ACCEPT_GENERAL_TERMS', 'FINISHED')
      AND identity_validation_status not in ('DENY','INVALID')
      ),
  ob_exitoso as (
   SELECT DISTINCT
      id,
      DATE(created_at, "America/Santiago") as fecha_creacion,
      DATE(onboarding_date, "America/Santiago") as fecha_ob,
      DATE(onboarding_date, "America/Santiago") as fecha_funnel,
      'OB Exitoso' paso,
      10 num,
      0 monto
    FROM users_savings
    WHERE 
     ( register_status in ('FINISHED') AND onboarding_status in ('FINISHED', 'READY', 'CP_SENT'))
      AND identity_validation_status not in ('DENY','INVALID')
      ),
      
   primer_ahorro as (
     SELECT DISTINCT
        user_id id,
        FIRST_VALUE(DATE(ci.created_date , "America/Santiago") ) OVER (PARTITION BY user_id ORDER BY DATE(ci.created_date , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_creacion,
        DATE(onboarding_date, "America/Santiago") as fecha_ob,
        FIRST_VALUE(DATE(ci.created_date , "America/Santiago") ) OVER (PARTITION BY user_id ORDER BY DATE(ci.created_date , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
        '1er ahorro' paso,
        11 num,
        FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY DATE(ci.created_date , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) monto
      FROM {{source('payment_savings','cash_in')}} ci
      JOIN users_savings u ON user_id = id
      WHERE 
        register_status in ('FINISHED')
        AND onboarding_status in ('FINISHED')
        AND ci.status IN ('SUCCESSFUL', 'PENDING', 'CONFIRMED', 'CREATED')
        AND identity_validation_status not in ('DENY','INVALID')
      ),
      
      
          
   primer_retiro as (
     SELECT DISTINCT
        user_id id,
        FIRST_VALUE(DATE(co.created_date , "America/Santiago") ) OVER (PARTITION BY user_id ORDER BY DATE(co.created_date , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_creacion,
        DATE(onboarding_date, "America/Santiago") as fecha_ob,
         FIRST_VALUE(DATE(co.created_date , "America/Santiago") ) OVER (PARTITION BY user_id ORDER BY DATE(co.created_date , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as fecha_funnel,
        '1er retiro' paso,
        12 num,
        FIRST_VALUE(amount) OVER (PARTITION BY user_id ORDER BY DATE(co.created_date , "America/Santiago") ASC
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) monto
      FROM {{source('payment_savings','cash_out')}} co
      JOIN users_savings u ON user_id = id
      WHERE 
        register_status in ('FINISHED')
        AND onboarding_status in ('FINISHED')
        AND co.status IN ('PENDING', 'CONFIRMED', 'CREATED', 'SUCCESSFUL', 'PENDING_SETTLEMENT')
        AND identity_validation_status not in ('DENY','INVALID')
      )
      
      
      
   SELECT * FROM inicio_ob UNION ALL
   SELECT * FROM inicio_ob_validacion_identidad UNION ALL
   SELECT * FROM ingresa_nacionalidad UNION ALL
   SELECT * FROM ingresa_status_trabajo UNION ALL
   SELECT * FROM ingresa_us_person UNION ALL
   SELECT * FROM ingresa_tax_residence UNION ALL
   SELECT * FROM ingresa_pep UNION ALL
   SELECT * FROM ingresa_origen_fondos UNION ALL
   SELECT * FROM ingresa_cp_related UNION ALL
   SELECT * FROM ingresa_tyc UNION ALL
   SELECT * FROM ob_exitoso UNION ALL
   SELECT * FROM primer_ahorro UNION ALL
   SELECT * FROM primer_retiro