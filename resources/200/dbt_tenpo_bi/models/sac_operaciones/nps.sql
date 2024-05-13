{{ 
  config(
    materialized='table', 
    tags=["hourly", "bi"],
  ) 
}}

WITH target AS
    (WITH score AS
            (SELECT 
                answer as score,
                item_id,
                date(submitted_at) AS submitted_at
            FROM {{source('typeform','typeform_item_answer')}}
            WHERE form_id = 'yXYjLgjX'
            AND field_type = 'opinion_scale'
            ORDER BY item_id DESC
            ),

        feedback AS
            (SELECT
                DISTINCT
                replace(answer,',,',' ') AS feedback
                ,item_id
                ,date(submitted_at) AS submitted_at
            FROM {{source('typeform','typeform_item_answer')}}
            where form_id = 'yXYjLgjX'
            and field_type = 'multiple_choice'
            order by item_id desc
        ),
            user as (SELECT
                REPLACE(REPLACE(answer, "87356-445960001-30-7657398233-49876-456765-98785324-set",""),"f809021-898765678765","") as rut,
                item_id,
                submitted_at
            FROM {{source('typeform','typeform_item_answer')}}
            where form_id = 'yXYjLgjX'
            and field_type = 'hidden'
            order by item_id desc)

        SELECT 
            DISTINCT 
            {{ hash_sensible_data('users.email') }} as email,
            users.id user,
            {{ hash_sensible_data('users.phone') }} as phone,
            users.first_name,
            CAST(score.score AS INT64) score,
            feedback.feedback as raw_feedback,
            CASE
                WHEN LOWER(feedback.feedback) like '%p,r,o,m,o,c,i,o,n,e,s%' or lower(feedback.feedback) like '%promociones%' then 'promociones_y_descuentos'                                   
                WHEN LOWER(feedback.feedback) like '%f,u,n,c,i,o,n,a,m,i,e,n,t,o%' or lower(feedback.feedback) like '%funcionamiento%' then 'funcionamiento_de_la_app'
                WHEN LOWER(feedback.feedback) like '%s,e,r,v,i,c,i,o%' or lower(feedback.feedback) like '%servicio%' then 'servicio_al_cliente'
                WHEN LOWER(feedback.feedback) like '%a,l,t,e,r,n,a,t,i,v,a,s%' or lower(feedback.feedback) like '%alternativas%' then 'alternativas_de_uso_dinero'
                WHEN LOWER(feedback.feedback) like '%f,a,c,i,l,i,d,a,d%' or lower(feedback.feedback) like '%facilidad%' then 'facilidad_uso_app'
                WHEN LOWER(feedback.feedback) like '%e,x,p,e,r,i,e,n,c,i,a%' or lower(feedback.feedback) like '%experiencia%' then 'experiencia_carga_retiro'
                WHEN LOWER(feedback.feedback) like '%o,t,r,o%' or lower(feedback.feedback) like '%otro%' then 'otro'
                end as feedback,
            'general' as type,
            score.submitted_at,
            UNIX_SECONDS(TIMESTAMP(score.submitted_at)) + 43200 as ts
        FROM score
        JOIN user using(item_id)
        LEFT JOIN feedback using(item_id)
        JOIN {{source('identity','ruts')}} ruts on ruts.rut_complete = user.rut
        JOIN {{source('tenpo_users','users')}} users using (tributary_identifier)
        ORDER by submitted_at desc) 

SELECT  
    DISTINCT
    target.*,
    CASE 
        WHEN score IN (9,10) THEN 'promotor'
        WHEN score IN (7,8) THEN 'neutro'
        ELSE 'detractor' END AS clasificacion,
        B.segmento_rfmp,
        B.invited as invitado_invita_y_gana,
        CASE WHEN referidos.user IS NULL THEN false ELSE true END AS invitado_por_referidor_i_y_g
FROM target
JOIN {{ ref('users_to_invite') }} B ON B.id = target.user
LEFT JOIN (SELECT DISTINCT user
                    FROM {{ ref('parejas_iyg') }}
            WHERE user is not null) referidos ON referidos.user = target.user
ORDER BY submitted_at DESC