{{ config( 
    tags=["daily", "bi"],
    materialized='table',
    schema='external',
    alias='partners_daily_android_ios')
}}

select *
from {{source('appsflyer_v2','ios_partners_daily_historico')}} where date <'2022-01-29'
union all
select *, 
null cupon_ingresado_con_exito_unique_users,
null cupon_ingresado_con_exito_event_counter,  
null cupon_ingresado_con_exito_sales_in_usd,
from {{source('appsflyer_v2','android_partners_daily_historico')}} where date <'2022-01-29'
union all 
select *,
null cupon_ingresado_con_exito_sales_in_usd,
null cupon_ingresado_con_exito_unique_users,
null cupon_ingresado_con_exito_event_counter,
null purchase_subscription_confirmed_sales_in_usd,
null ob_intent_event_counter,
null ob_intent_sales_in_usd,
null ob_intent_unique_users,
null pago_de_cuentas_exitoso_event_counter,
null purchase_subscription_confirmed_event_counter,
null cupon_redimido_con_exito_event_counter,
null cupon_redimido_con_exito_unique_users,
null pago_de_cuentas_exitoso_sales_in_usd,
null purchase_subscription_confirmed_unique_users,
null pago_de_cuentas_exitoso_unique_users,
null enter_app_unique_users,
null enter_app_event_counter,
null cupon_redimido_con_exito_sales_in_usd,
null enter_app_sales_in_usd, from {{source('appsflyer_v2','partners_daily_android')}}
union all
select *,
null cupon_ingresado_con_exito_sales_in_usd,
null cupon_ingresado_con_exito_unique_users,
null cupon_ingresado_con_exito_event_counter,
null purchase_subscription_confirmed_sales_in_usd,
null ob_intent_event_counter,
null ob_intent_sales_in_usd,
null ob_intent_unique_users,
null pago_de_cuentas_exitoso_event_counter,
null purchase_subscription_confirmed_event_counter,
null cupon_redimido_con_exito_event_counter,
null cupon_redimido_con_exito_unique_users,
null pago_de_cuentas_exitoso_sales_in_usd,
null purchase_subscription_confirmed_unique_users,
null pago_de_cuentas_exitoso_unique_users,
null enter_app_unique_users,
null enter_app_event_counter,
null cupon_redimido_con_exito_sales_in_usd,
null enter_app_sales_in_usd, from {{source('appsflyer_v2','partners_daily_ios')}}