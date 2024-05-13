with
B1 as (select distinct id,tributary_identifier from `{project_source_3}.users.users`),
B2 as (select distinct ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)] id FROM `{project_source_4}.clevertap_raw.clevertap_gold_external`
        where eventname in ('Vinculacion exitosa PayPal','Vinculación exitosa PayPal')),
B3 as (select distinct ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)] id FROM `{project_source_4}.clevertap_raw.clevertap_gold_external`
        where eventprops.description = 'Pantalla exito vinculacion billetera dólares'),
B4 as (SELECT distinct rut FROM `{project_source_1}.paypal.transacciones_paypal` where tip_trx ='ABONO_APP_PAYPAL'),
SEGMENTO_1 as (select distinct 'i' as type,a.id as identity from B1 a join B2 b on a.id = b.id
                                  left join B3 c on a.id = c.id
                                  left join B4 d on a.tributary_identifier = d.rut
                where c.id is null
                      and d.rut is null)
select * from SEGMENTO_1