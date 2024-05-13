{{
    config(
        materialized='table'
    )
}}

WITH all_users AS

(SELECT 
    DISTINCT
    A.email, 
    A.no_molestar,
    A.reason
FROM {{ ref('today_untouchable_users') }} A
--LEFT JOIN {{ ref('today_churn_users') }} B ON A.email = B.email
LEFT JOIN {{ ref('today_open_tickets') }} C ON A.email = C.email
--LEFT JOIN {{ ref('today_open_tickets_churn') }} D on A.email = D.email
--WHERE B.email IS NULL --### EXCLUIR LOS QUE SON CHURN 
WHERE C.email IS NULL --### EXCLUIR LOS QUE SON OPEN TICKETS
--AND D.email IS NULL --### EXCLUIR LOS QUE SON CHURN Y OPEN TICKETS

UNION ALL 

(SELECT 
        DISTINCT
        A.email, 
        A.no_molestar,
        A.reason
FROM {{ ref('today_touchable_users') }} A

--LEFT JOIN {{ ref('today_churn_users') }} B ON A.email = B.email
LEFT JOIN {{ ref('today_open_tickets') }} C ON A.email = C.email
--LEFT JOIN {{ ref('today_open_tickets_churn') }} D on A.email = D.email
--WHERE B.email IS NULL 
WHERE C.email IS NULL)
--AND D.email IS NULL) 


/*
UNION ALL 
(SELECT 
        DISTINCT
        A.email, 
        A.no_molestar,
        A.reason
FROM {{ ref('today_churn_users') }} A
LEFT JOIN {{ ref('today_open_tickets_churn') }}  B ON A.email = B.email
LEFT JOIN {{ ref('today_open_tickets') }}  C ON A.email = C.email
WHERE B.email IS NULL 
AND C.email IS NULL )
*/

UNION ALL 
(SELECT 
        DISTINCT
        A.email, 
        A.no_molestar,
        A.reason
FROM {{ ref('today_open_tickets') }} A))

--LEFT JOIN {{ ref('today_churn_users') }} B ON A.email = B.email
--LEFT JOIN {{ ref('today_open_tickets_churn') }}  C ON A.email = C.email
--WHERE B.email IS NULL 
--AND C.email IS NULL) 

/*
UNION ALL 
(SELECT 
        DISTINCT
        A.email, 
        A.no_molestar,
        A.reason
FROM {{ ref('today_open_tickets_churn') }} A
LEFT JOIN {{ ref('today_churn_users') }} B ON A.email = B.email
LEFT JOIN {{ ref('today_open_tickets') }} C ON A.email = C.email
WHERE B.email IS NULL 
AND C.email IS NULL )
*/

SELECT DISTINCT *, CURRENT_DATE() AS close_date_analysis FROM all_users