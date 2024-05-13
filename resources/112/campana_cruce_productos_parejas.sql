DROP TABLE IF EXISTS `tenpo-bi.tmp.campana_cruce_parejas_{{ds_nodash}}`;
CREATE TABLE `tenpo-bi.tmp.campana_cruce_parejas_{{ds_nodash}}` AS (

    with nombre_pareja as 
    (
    SELECT id_cliente as identity, nombre_pareja as campana_cruce_parejas_nombre_pareja
        ,n_productos_cliente as campana_cruce_parejas_n_productos_cliente
        ,n_productos_pareja as campana_cruce_parejas_n_productos_pareja
    FROM `tenpo-sandbox.crm.borrar15noviembre_campana_cruce_octubre_avance_parejas`   
    )
    select
        a.id as identity
        ,ifnull(b.campana_cruce_parejas_nombre_pareja,'no tiene pareja') as campana_cruce_parejas_nombre_pareja
        ,ifnull(b.campana_cruce_parejas_n_productos_cliente,0) as campana_cruce_parejas_n_productos_cliente
        ,ifnull(b.campana_cruce_parejas_n_productos_pareja,0) as campana_cruce_parejas_n_productos_pareja
    from `tenpo-bi-prod.users.users_tenpo` a
    left join nombre_pareja b on a.id = b.identity
    where a.status_onboarding = 'completo' 
)