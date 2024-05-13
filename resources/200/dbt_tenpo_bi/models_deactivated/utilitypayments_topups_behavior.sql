{{ config(materialized='table',  tags=["daily", "bi"]) }}

WITH 
  datos AS (
    WITH 
    tenpo_utilitypayments AS (
      select distinct 
        "utility_payment" as tipo,
        users.created_at	as user_created_at,
        md5(trx.user) as user,
        payments.id,
        payments.status,
        payments.amount,
        payments.created as payment_created,
        EXTRACT(DAY FROM CAST(payments.created AS DATE)) as day_created,
        EXTRACT(DAYOFWEEK FROM CAST(payments.created AS DATE)) as dayofweek_created,
        categories.name as category_name,
        utilities.name as utility_name,
        count(distinct payments.id) OVER (PARTITION BY EXTRACT(MONTH FROM CAST(payments.created AS DATE)) ) as total_payments_mes,
        count(distinct trx.user) OVER (PARTITION BY EXTRACT(MONTH FROM CAST(payments.created AS DATE)) ) as total_users_mes,
        date_diff(FIRST_VALUE(CAST(payments.created AS DATE)) OVER (PARTITION BY trx.user ORDER BY payments.created),CAST(users.created_at AS DATE),DAY) as days_to_first_payment,
      from `tenpo-airflow-prod.tenpo_utility_payment.transactions`  as trx
        join `tenpo-airflow-prod.tenpo_utility_payment.payments` as payments ON trx.id=payments.transaction_id 
        join `tenpo-airflow-prod.tenpo_utility_payment.bills` as bills ON payments.bill_id=bills.id
        join `tenpo-airflow-prod.tenpo_utility_payment.utilities`as utilities on bills.utility_id = utilities.id 
        join `tenpo-airflow-prod.tenpo_utility_payment.categories` as categories on utilities.category_id = categories.id 
        join `tenpo-airflow-prod.users.users` users on trx.user = users.id
    ),
    tenpo_topups AS (
      select distinct
        "topup" as tipo,
        users.created_at	as user_created_at,
        md5(trx.user_id) as user,
        payments.id,
        payments.status,
        topups.amount  amount,
        payments.created_at as payment_created,
        EXTRACT(DAY FROM CAST(payments.created_at AS DATE)) as day_created,
        EXTRACT(DAYOFWEEK FROM CAST(payments.created_at AS DATE)) as dayofweek_created,
        product.name as category_name,
        operator.name as utility_name,
        count(distinct payments.id) OVER (PARTITION BY EXTRACT(MONTH FROM CAST(payments.created_at AS DATE)) ) as total_payments_mes,
        count(distinct trx.user_id) OVER (PARTITION BY EXTRACT(MONTH FROM CAST(payments.created_at AS DATE)) ) as total_users_mes,
        date_diff(FIRST_VALUE(CAST(payments.created_at AS DATE)) OVER (PARTITION BY trx.user_id ORDER BY payments.created_at),CAST(users.created_at AS DATE),DAY) as days_to_first_payment,
      from `tenpo-airflow-prod.tenpo_topup.transaction`  as trx
        join `tenpo-airflow-prod.tenpo_topup.topup` as topups ON trx.id=topups.transaction_id
        join `tenpo-airflow-prod.tenpo_topup.payment` as payments ON topups.id=payments.topup_id  
        join `tenpo-airflow-prod.tenpo_topup.product_operator` as product_operator on product_operator.id  = topups.product_operator_id 
        join `tenpo-airflow-prod.tenpo_topup.product` as product on product.id = product_operator.product_id  
        join `tenpo-airflow-prod.tenpo_topup.product` as operator on operator.id = product_operator.operator_id
        join `tenpo-airflow-prod.users.users` users on trx.user_id = users.id
    )
    select * from tenpo_utilitypayments
    UNION ALL
    select * from tenpo_topups 
)
SELECT DISTINCT * FROM datos