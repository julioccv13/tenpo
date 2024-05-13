DROP TABLE IF EXISTS `${project_target}.tmp.query_politica_toque_{{ds_nodash}}`;
CREATE TABLE `${project_target}.tmp.query_politica_toque_{{ds_nodash}}` AS (

WITH
B1 AS (SELECT ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)] as identity,
        case when eventprops.campaign_id is null then LEFT(REPLACE(eventprops.wzrk_id,'_',''),
        (LENGTH(REPLACE(eventprops.wzrk_id,'_','')        )-8)) else eventprops.campaign_id end campaign_id, 
        case when eventprops.campaign_type in ('Mobile Push - iOS','Mobile Push - Android',
        'Mobile "Push - iOS"','Mobile "Push - Android"') then 'Push' else eventprops.campaign_type END canal_campaign, 
        dt,
        right(eventprops.wzrk_id, 8) start_campaign,
       -- count(distinct profile.identity) N
--select eventprops
      FROM `tenpo-bi-prod.external.clevertap_raw`
      WHERE eventname in ('Notification Sent','Notification Viewed') 
        AND (length(eventprops.campaign_id) = 10 OR CAST(length(LEFT(REPLACE(eventprops.wzrk_id,'_',''), 
            (LENGTH(REPLACE(eventprops.wzrk_id,'_',''))-8))) AS INT64) = 10)
        AND DT >= date_add(CURRENT_DATE(),INTERVAL -1 MONTH)
--      GROUP BY 1,2,3,4
--      HAVING count(distinct profile.identity) > 100
      ORDER BY 3),
Matriz AS (SELECT * FROM B1 a 
        LEFT JOIN `tenpo-sandbox.crm.Proyecto_Efectividad_Campana_TABLE_refactor` B
        ON cast(a.campaign_id as int) =  b.Id_Campana
        WHERE B.Politica_Toques = 'Incluir Politica Toques'),

Matriz_Producto as (SELECT identity,case when producto in ('Tyba','Bolsillo','Tyba/Bolsillo')                      then 'Inversion'
                      when producto in ('Tarjeta','Solo Tarjeta Fisica','Solo Tarjeta Virtual') then 'Tarjeta'
                      when producto = 'PDC'                                                     then 'Pdc'
                      when producto = 'REC'                                                     then 'Rec'
                      when producto = 'PayPal'                                                  then 'Paypal'
                      when producto = 'Remesas'                                                 then 'Remesas'
                      when producto = 'Tenpo Business'                                          then 'Tb'
                      when producto = 'Seguros'                                                 then 'Seguros'
                      when producto = 'Payroll'                                                 then 'Payroll'
                      when producto in ('CashinFisico','Cashout','Cashin')                      then 'Cash'
                      when producto = 'P2P'                                                     then 'P2p'
                      when producto = 'Reglas Automaticas'                                      then 'Ra'
                      when producto = 'Invita y Gana'                                           then 'Iyg'
                      when producto = 'OB'                                                      then 'Ob'
                      when producto = 'Multiproducto'                                           then 'Cuenta_Multiproducto' 
                                  else 'n/a' end M_Producto,
                                  count(*) N
                      FROM Matriz
                      WHERE identity is not null
                      GROUP BY 1,2
                      ORDER BY 1,2),
Matriz_Email as (SELECT identity,count(*) cuenta_email FROM Matriz
                  WHERE canal = 'Email'
                  GROUP BY 1
                  HAVING COUNT(*) <= 5
                  ORDER BY 2 )
SELECT a.id as identity, 
    case when b.identity is not null then b.cuenta_email else 0 end toque_email,
    case when c.M_Producto = 'Inversion' then c.N else 0 end toque_inversion,
    case when c.M_Producto = 'Tarjeta' then c.N else 0 end toque_tarjeta,
    case when c.M_Producto = 'Pdc' then c.N else 0 end toque_pdc,
    case when c.M_Producto = 'Rec' then c.N else 0 end toque_rec,
    case when c.M_Producto = 'Paypal' then c.N else 0 end toque_paypal,
    case when c.M_Producto = 'Remesas' then c.N else 0 end toque_remesas,
    case when c.M_Producto = 'Tb' then c.N else 0 end toque_tb,
    case when c.M_Producto = 'Seguros' then c.N else 0 end toque_seguros,
    case when c.M_Producto = 'Payroll' then c.N else 0 end toque_payroll,
    case when c.M_Producto = 'Cash' then c.N else 0 end toque_cash,
    case when c.M_Producto = 'P2p' then c.N else 0 end toque_p2p,
    case when c.M_Producto = 'Ra' then c.N else 0 end toque_ra,
    case when c.M_Producto = 'Iyg' then c.N else 0 end toque_iyg,
    case when c.M_Producto = 'Ob' then c.N else 0 end toque_ob,
    case when c.M_Producto = 'Cuenta_Multiproducto' then c.N else 0 end toque_cm,
    case when b.cuenta_email >= 6 then 0 else 1 end plt_email,
    case when (c.M_Producto = 'Inversion' and c.N >= 2) then 0 else 1 end plt_inversion,
    case when (c.M_Producto = 'Tarjeta' and c.N >= 3) then 0 else 1 end plt_tarjeta,
    case when (c.M_Producto = 'Pdc' and c.N >= 3) then 0 else 1 end plt_pdc,
    case when (c.M_Producto = 'Rec' and c.N >= 2) then 0 else 1 end plt_rec,
    case when (c.M_Producto = 'Paypal' and c.N >= 2) then 0 else 1 end plt_paypal,
    case when (c.M_Producto = 'Remesas' and c.N >= 2) then 0 else 1 end plt_remesas,
    case when (c.M_Producto = 'Seguros' and c.N >= 2) then 0 else 1 end plt_seguros,
    case when (c.M_Producto = 'Cash' and c.N >= 2) then 0 else 1 end plt_cash,
    case when (c.M_Producto = 'P2p' and c.N >= 2) then 0 else 1 end plt_p2p,
    case when (c.M_Producto = 'Iyg' and c.N >= 2) then 0 else 1 end plt_iyg
FROM `tenpo-bi-prod.users.users_tenpo` a 
LEFT JOIN Matriz_Email b on a.id = b.identity
LEFT JOIN Matriz_Producto c on a.id = c.identity
);
