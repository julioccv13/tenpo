DROP TABLE IF EXISTS `${project_target}.jarvis.event_string_query_{{ds_nodash}}`;

CREATE TABLE `${project_target}.jarvis.event_string_query_{{ds_nodash}}` AS (

with primer_producto_recomendador as
(
    select user, recomendacion_1
    from `${project_source_1}.modelos_ds.consolidado_recomendacion_user`
    where fecha_ejecucion = (select max(fecha_ejecucion) from `${project_source_1}.modelos_ds.consolidado_recomendacion_user`)
)
,tipo_mau as
(
    select user, tipo_mau_mes
    from `${project_source_1}.campaign_management.funnel_users_mau_type_history`
    where fecha = (select max(fecha) from `${project_source_1}.campaign_management.funnel_users_mau_type_history`)
)
,segmento_cliente as
(
    with segmento_pre
    as
    (
    select distinct user, segmento
    from `${project_source_1}.prepago.accounts`
    UNION ALL
    select distinct a.id as user,
        case
          when a.plan = 0 then 'PERSONAL'
          when a.plan = 1 then 'JR'
          when a.plan = 2 then 'JR'
          when a.plan = 3 then 'JR' else 'otro/sin categorizar' 
        end as segmento
    from `${project_source_1}.users.users_tenpo` a
    left join `${project_source_1}.prepago.accounts` b on a.id = b.user
    where b.user is null 
    and (a.plan = 0 or a.plan = 1 or a.plan = 2 or a.plan = 3)
    )
    ,segmento as (select distinct user, segmento from segmento_pre order by 2 desc)
    select 
      user, 
      max(a.segmento) as segmento_cliente
    from segmento a
    group by 1
)
,producto_top1_historico as --dejo fuera cashin y cashout, tomo toda la historia de la economics
(
    with pre as
    (
        select user, linea, count(1) as n_txs
        from `${project_source_1}.economics.economics`
        where linea in ('investment_tyba','cash_in_savings','utility_payments','top_ups','paypal_abonos','paypal','p2p','mastercard_physical','mastercard','insurance','crossborder')
        AND linea NOT LIKE '%PFM%'
        AND nombre NOT LIKE '%Home%'
        AND linea != 'Saldo'
        AND linea != 'saldo'
        and lower(nombre) not like '%devoluc%'
        AND linea <> 'reward'
        group by 1,2
    )
    ,pre_2 as
    (select user, linea, n_txs,ROW_NUMBER() OVER (PARTITION BY user order by n_txs desc) as rank
    from pre)
    select user, linea
    from pre_2
    where rank = 1
)
,productos_activos as --aca tomos los productos en que ha sido mau en el mes. incluyo cashin, cashout y los aum de bolsillo y tyba
(
    with target as (
        select distinct 
        user,
        case
          when linea = 'investment_tyba' then 'ffmm'
          when linea = 'aum_tyba' then 'ffmm'
          when linea = 'withdrawal_tyba' then 'ffmm'
          when linea = 'cash_in_savings' then 'bolsillo'
          when linea = 'aum_savings' then 'bolsillo'
          when linea = 'cash_out_savings' then 'bolsillo'
          when linea = 'credit_card_physical' then 'tc_fisica'
          when linea = 'credit_card_virtual' then 'tc_virtual'
          when linea = 'utility_payments' then 'pdc'
          when linea = 'top_ups' then 'rec'
          when linea = 'paypal_abonos' then 'paypal'
          when linea = 'paypal' then 'paypal'
          when linea = 'mastercard_physical' then 'master_fisica'
          when linea = 'mastercard' then 'master_digital'
          when linea = 'insurance' then 'seguro'
          when linea = 'cash_in' then 'cashin'
          when linea = 'cash_out' then 'cashout'
          when linea = 'p2p' then 'p2p'
          when linea = 'crossborder' then 'remesa' else linea end as linea
        from `${project_source_1}.economics.economics`
        where
        date_trunc(current_date(),month) = date_trunc(fecha,month)
        and linea in ('credit_card_physical','credit_card_virtual','aum_tyba','aum_savings','cash_in','cash_out','investment_tyba','cash_in_savings','utility_payments','top_ups','paypal_abonos','paypal','p2p','mastercard_physical','mastercard','insurance','crossborder','withdrawal_tyba','cash_out_savings')
        ),
      products AS (
        SELECT
            *
        FROM target
        ORDER BY user, linea desc
        )
        select 
              user,
              STRING_AGG(a.linea, "|") as productos_activos
        from products a
        group by 1
),
pseudo_user_id as
(
  SELECT 
      *
  FROM `${project_source_1}.analytics_firebase.pseudonymous_users` 
),
primera_activacion as
(
    with pre as
    (
    SELECT distinct
    user_id,
    producto,
    ROW_NUMBER() OVER (PARTITION BY user_id, objetivo order by score desc) as rank
    FROM `${project_source_2}.jarvis.recommendation_activation_product`
    where date(fecha_ejecucion) = date(current_date()) and objetivo = 'PRIMERA_ACTIVACION'
    )
    select user_id,producto from pre where rank = 1
),
reactivacion as
(
    with pre as
    (
    SELECT distinct
    user_id,
    producto,
    ROW_NUMBER() OVER (PARTITION BY user_id, objetivo order by score desc) as rank
    FROM `${project_source_2}.jarvis.recommendation_activation_product`
    where date(fecha_ejecucion) = date(current_date()) and objetivo = 'REACTIVACION'
    )
    select user_id,producto from pre where rank = 1
),
prod_me_gusta as
(
SELECT
  user_id,
  STRING_AGG(a.product_name, "|") as producto_me_gusta
FROM
  (select distinct user_id, product_name
    from `${project_source_1}.assessment_db.product_user_reaction`
    where reaction = 'LIKE' order by user_id,product_name desc) a
group by 1
),
prod_me_encanta as
(
SELECT
  user_id,
  STRING_AGG(a.product_name, "|") as producto_me_encanta
FROM
  (select distinct user_id, product_name
    from `${project_source_1}.assessment_db.product_user_reaction`
    where reaction = 'LOVE' order by user_id,product_name desc) a
group by 1
),
prod_no_es_para_mi as
(
SELECT
  user_id,
  STRING_AGG(a.product_name, "|") as producto_no_es_para_mi
FROM
  (select distinct user_id, product_name
    from `${project_source_1}.assessment_db.product_user_reaction`
    where reaction = 'DISLIKE' order by user_id,product_name desc) a
group by 1
)
SELECT
    a.id_usuario as user
    ,a.edad AS edad
    ,ifnull(a.gender, 'sin_información') AS genero
    ,ifnull(c.tipo_mau_mes,'no mau mes actual') as tipo_mau_mes
    ,ifnull(d.segmento_cliente,'sin segmento') as segmento_cliente
    ,ifnull(e.linea,'sin producto top') as producto_top1_historico
    ,ifnull(f.productos_activos,'no mau mes actual') as productos_activos_mes_actual
    ,g.pseudo_user_id
    ,g.operating_system
    ,ifnull(i.producto, 'sin_producto_a_activar') AS producto_a_activar
    ,ifnull(j.producto, 'sin_producto_a_reactivar') AS producto_a_reactivar
    ,ifnull(k.producto_me_gusta, 'sin_producto_me_gusta') AS producto_me_gusta
    ,ifnull(l.producto_me_encanta, 'sin_producto_me_encanta') AS producto_me_encanta
    ,ifnull(m.producto_no_es_para_mi, 'sin_producto_no_es_para_mi') AS producto_no_es_para_mi
FROM `${project_source_1}.users.demographics` a
    left join primer_producto_recomendador b on a.id_usuario = b.user
    left join tipo_mau c on a.id_usuario = c.user
    left join segmento_cliente d on a.id_usuario = d.user
    left join producto_top1_historico e on a.id_usuario = e.user
    left join productos_activos f on a.id_usuario = f.user
    left join pseudo_user_id g on a.id_usuario = g.user
    left join primera_activacion i on a.id_usuario = i.user_id
    left join reactivacion j on a.id_usuario = j.user_id
    left join prod_me_gusta k on a.id_usuario = k.user_id
    left join prod_me_encanta l on a.id_usuario = l.user_id
    left join prod_no_es_para_mi m on a.id_usuario = m.user_id
where g.pseudo_user_id is not null AND a.edad IS NOT NULL
);


