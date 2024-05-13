WITH pivot_hooks AS (

    SELECT 
        * 
    --FROM `tenpo-bi-prod.marketing.hook_first_transaction`
    FROM {{ ref('hook_first_transaction') }} 

    UNION ALL 
    
    SELECT 
        * 
    --FROM `tenpo-bi-prod.marketing.hook_first_cashin`
    FROM {{ ref('hook_first_cashin') }} 


)


SELECT  fecha,
          SUM(case when motor='desconocido' then monto_usd else 0 end) as desconocido,
          SUM(case when motor='invita y gana' then monto_usd else 0 end) as invita_y_gana,
          SUM(case when motor='organic' then monto_usd else 0 end) organic,
          SUM(case when motor='paid media' then monto_usd else 0 end) paid_media,
          SUM(case when motor='partnership' then monto_usd else 0 end) partnership,
          SUM(case when motor='restricted_channel' then monto_usd else 0 end) restricted_channel,
          SUM(case when motor='xsell_paypal' then monto_usd else 0 end) xsell_paypal,
          SUM(case when motor='xsell_recargas' then monto_usd else 0 end) xsell_recargas 
FROM pivot_hooks
GROUP BY fecha ORDER BY fecha DESC


