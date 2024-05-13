with B1 as (select rut from `{project_source_1}.paypal.detalle_migrados_pp` where tip_trx in ('ABONO','RETIRO')),
B2 as (select distinct tributary_identifier,id from `{project_source_3}.users.users`),
B3 as (SELECT fecha_ob,uuid FROM `{project_source_1}.funnel.funnel_tenpo` where paso = '7. OB exitoso'
        union all
        SELECT fecha_ob,uuid FROM `{project_source_1}.funnel.funnel_tenpo_ob_ligth` where paso = '10. OB exitoso'),
B4 as (select distinct ARRAY_REVERSE(SPLIT(profile.identity))[SAFE_OFFSET(0)] id FROM `{project_source_4}.clevertap_raw.clevertap_gold_external`
        where eventname in ('Vinculacion exitosa PayPal','Vinculación exitosa PayPal') 
              or eventprops.description = 'Pantalla exito vinculacion billetera dólares'),
B5 as ((select distinct user from  `{project_source_1}.economics.economics` where linea in ('paypal','paypal_abonos'))),
SEGMENTO_1 as (select distinct 'i' as type, b.id as identity from B1 a 
                        join B2 b on a.rut = b.tributary_identifier
                        join B3 c on b.id = c.uuid
                        left join B4 d on b.id = d.id 
                        left join B5 e on b.id = e.user
                where d.id is null 
                and e.user is null
                and fecha_ob >=date_add(CURRENT_DATE(),INTERVAL -2 DAY))

select * from SEGMENTO_1