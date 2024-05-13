{{ config(tags=["hourly", "bi"], materialized='table') }}


SELECT 
      A.created
      ,A.id
      ,A.status
      ,A.user
      ,A.phone
      ,A.email
      ,B.state
      ,B.tributary_identifier
      ,B.profession
      ,B.address
      ,B.category
      ,date(B.ob_completed_at,"America/Santiago") ob_completed_at
      ,case 
        when status = 'COMPLETED' and B.tributary_identifier is not null and B.profession is not null and address is not null and category in ('B1','C1') then 'registro exitoso'
      else 'registro incompleto' end as status_onboarding
      ,C.fecha_fci
FROM {{ ref('onboarding_ligth') }} A
LEFT JOIN {{ ref('users_tenpo') }} B ON A.user = B.id
LEFT JOIN (

    SELECT 
      user,
      fecha as fecha_fci
    FROM {{ ref('economics') }}
    WHERE linea = 'cash_in'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY user ORDER BY trx_timestamp ASC) = 1

) C ON C.user = A.user

ORDER BY 1 DESC
