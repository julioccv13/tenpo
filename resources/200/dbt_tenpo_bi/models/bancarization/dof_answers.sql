{{ 
  config(
    materialized='table', 
  ) 
}}


WITH  dof_created AS 

(SELECT 
        DISTINCT
        A.created_at as dof_created,
        A.updated_at dof_updated,
        A.user_id AS user,
        A.id as dof_id,
        A.account_id,
        D.uuid as trx_id,
        B.amount, 
        B.dollar_amount, 
        B.date AS transaction_date,
        B.fund_source,
        B.created_at AS transaction_created_at,
        B.updated_at as transaction_updated_at,
        C.message,
        A.status,
        --C.status,
        C.created_at AS notification_created_at,
FROM {{ source('transaction_verification', 'dof') }} A --`tenpo-airflow-prod.transaction_verification.dof`
JOIN {{ source('transaction_verification', 'transaction') }} B ON A.id = B.dof_id --`tenpo-airflow-prod.transaction_verification.transaction`
JOIN {{ source('transaction_verification', 'notification') }} C on A.id = C.dof_id --`tenpo-airflow-prod.transaction_verification.notification`
JOIN {{ source('prepago', 'prp_movimiento') }} D on B.id = D.id_tx_externo --`tenpo-airflow-prod.prepago.prp_movimiento`
WHERE C.message = 'DOF_CREATED'
QUALIFY ROW_NUMBER() OVER (PARTITION BY A.user_id,dof_id ORDER BY  C.created_at ASC) = 1
ORDER BY user_id , C.created_at DESC

), dof_last_status AS (

        SELECT 
                DISTINCT
                A.created_at as dof_created,
                A.updated_at dof_updated,
                A.user_id AS user,
                A.id as dof_id,
                A.account_id,
                D.uuid as trx_id,
                B.amount, 
                B.dollar_amount, 
                B.date AS transaction_date,
                B.fund_source,
                B.created_at AS transaction_created_at,
                C.message,
                A.status,
                --C.status,
                C.created_at AS notification_created_at,
        FROM {{ source('transaction_verification', 'dof') }} A --`tenpo-airflow-prod.transaction_verification.dof`
        JOIN {{ source('transaction_verification', 'transaction') }} B ON A.id = B.dof_id --`tenpo-airflow-prod.transaction_verification.transaction`
        JOIN {{ source('transaction_verification', 'notification') }} C on A.id = C.dof_id --`tenpo-airflow-prod.transaction_verification.notification`
        JOIN {{ source('prepago', 'prp_movimiento') }} D on B.id = D.id_tx_externo --`tenpo-airflow-prod.prepago.prp_movimiento`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY A.user_id, dof_id ORDER BY  C.created_at DESC) = 1
        ORDER BY user_id , C.created_at DESC

)

SELECT 
    DISTINCT
    dof_created.dof_created,
    dof_created.dof_updated,
    dof_created.user,
    dof_created.dof_id,
    dof_created.trx_id,
    dof_last_status.status,
    dof_created.amount,
    dof_created.dollar_amount,
    dof_created.transaction_date,
    dof_created.fund_source,
    dof_created.message AS first_state,
    dof_last_status.message AS last_state,
    dof_created.notification_created_at as first_state_created_at,
    dof_last_status.notification_created_at last_state_created_at,
    DATE_DIFF(dof_last_status.notification_created_at ,  dof_created.notification_created_at, MINUTE) mins_to_answer,
    DATE_DIFF(dof_last_status.notification_created_at ,  dof_created.notification_created_at, HOUR) hours_to_answer,
    DATE_DIFF(dof_last_status.notification_created_at ,  dof_created.notification_created_at, DAY) days_to_answer,
    DATE_DIFF(current_date(), DATE(dof_created.notification_created_at), DAY)  days_from_today
FROM dof_created
LEFT JOIN dof_last_status ON dof_created.dof_id = dof_last_status.dof_id
ORDER BY user, dof_created DESC

