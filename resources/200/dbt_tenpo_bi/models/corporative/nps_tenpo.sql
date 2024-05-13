{{ config(materialized='table',  tags=["daily", "bi"]) }}

WITH 
  typeform_prepago as (
    SELECT 
        CASE 
         WHEN a.form_id  = 'FGW8nJ' THEN 'recargas' 
         WHEN a.form_id = 'ox9et1' THEN 'retiros'
         WHEN a.form_id = 'ngmGeQ' THEN 'abonos'
         WHEN a.form_id = 'rTD21W' THEN 'prepago' END as linea,
        a.form_id,
        CASE 
         WHEN field_ref = '1ce59591-4f5d-4249-96a8-ebc4e727ff41' THEN 'pregunta_nps' 
         WHEN field_ref = 'fe18ce31-05a4-4838-86ea-925472ce119d' THEN 'pregunta_mejora' END as pregunta,
        IF(field_type = 'rating' AND field_ref = '1ce59591-4f5d-4249-96a8-ebc4e727ff41' AND cast(answer as int64) in (9,10) , 'promotor', 
        IF(field_type = 'rating' AND field_ref = '1ce59591-4f5d-4249-96a8-ebc4e727ff41' AND cast(answer as int64) in (7,8) , 'neutro',
        IF(field_type = 'rating' AND field_ref = '1ce59591-4f5d-4249-96a8-ebc4e727ff41' AND cast(answer as int64) in (0,1,2,3,4,5,6) , 'detractor', 
        IF(field_type = 'multiple_choice' AND field_ref = 'fe18ce31-05a4-4838-86ea-925472ce119d' , answer, null)))) respuesta,
        cast(i.submitted_at as date) as fecha,
        FORMAT_DATE('%Y-%m-01', cast(i.submitted_at as date)) mes,
    FROM {{source('typeform','typeform_item_answer')}}   a -- `tenpo-external.typeform.typeform_item_answer`
    JOIN {{source('typeform','typeform_item')}}  i USING(item_id) --`tenpo-external.typeform.typeform_item`
    WHERE 
        a.form_id  = 'rTD21W'
    ),
  typeform_nndd as (
    SELECT 
        CASE 
         WHEN a.form_id  = 'FGW8nJ' THEN 'recargas' 
         WHEN a.form_id = 'ox9et1' THEN 'retiros'
         WHEN a.form_id = 'ngmGeQ' THEN 'abonos'
        WHEN a.form_id = 'rTD21W' THEN 'prepago' END as linea,
        a.form_id,
        CASE 
         WHEN field_ref in ('94db45d8-cdad-4a05-8274-359974f41759' ,'3991bb2b-c1b9-44c9-8a75-4a4e99c4d496') THEN 'pregunta_nps' 
         WHEN field_ref in ('d5650e10-136e-46e1-8da2-8d4611609545', 'e5f5cd93-dee1-4835-b432-0fa0f67e0cad') THEN 'pregunta_mejora' END as pregunta,
        if(field_type = 'opinion_scale' AND field_ref in ('94db45d8-cdad-4a05-8274-359974f41759' ,'3991bb2b-c1b9-44c9-8a75-4a4e99c4d496') AND cast(answer as int64) in (9,10) , 'promotor', 
        if(field_type = 'opinion_scale' AND field_ref in ('94db45d8-cdad-4a05-8274-359974f41759' ,'3991bb2b-c1b9-44c9-8a75-4a4e99c4d496') AND cast(answer as int64) in (7,8) , 'neutro',
        if(field_type = 'opinion_scale' AND field_ref in ('94db45d8-cdad-4a05-8274-359974f41759' ,'3991bb2b-c1b9-44c9-8a75-4a4e99c4d496') AND cast(answer as int64) in (0,1,2,3,4,5,6) , 'detractor' , 
        if(field_type in ('short_text','multiple_choice') AND field_ref in ('d5650e10-136e-46e1-8da2-8d4611609545', 'e5f5cd93-dee1-4835-b432-0fa0f67e0cad') , answer, null)))) respuesta,
        cast(i.submitted_at as date) as fecha,
        format_date('%Y-%m-01', cast(i.submitted_at as date)) mes,
    FROM {{source('typeform','typeform_item_answer')}}   a -- `tenpo-external.typeform.typeform_item_answer`
    JOIN {{source('typeform','typeform_item')}}  i USING(item_id) --`tenpo-external.typeform.typeform_item`
    WHERE 
        a.form_id  in ( 'FGW8nJ', 'ngmGeQ', 'ox9et1'))

SELECT 
   linea, 
   form_id, 
   pregunta, 
   respuesta, 
   CASE 
    WHEN respuesta = 'promotor' THEN 1 
    WHEN respuesta = 'detractor' THEN -1 
    WHEN respuesta = 'neutro' THEN 0 ELSE null 
    END AS resp_recod,
    fecha,
    mes
FROM typeform_nndd 
UNION ALL 
    (SELECT 
     linea, 
     form_id, 
     pregunta, 
     respuesta, 
    CASE 
     WHEN respuesta = 'promotor' THEN 1 
     WHEN respuesta = 'detractor' THEN -1 
     WHEN respuesta = 'neutro' THEN 0 ELSE null 
     END AS resp_recod,  
     fecha,
     mes 
   FROM typeform_prepago)
------------------>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  NUEVO NPS  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<--------------------   
UNION ALL 
   
(
WITH 
    nps AS (
        SELECT 
           'prepago' as linea,
           form_id,
           CASE  
              WHEN field_ref = '1a3b8ab5-24d5-4b8b-a457-b8fc45725f9c' THEN 'pregunta_nps'
              ELSE 'pregunta_mejora' 
              END AS pregunta,
           CASE
              WHEN field_ref = '1a3b8ab5-24d5-4b8b-a457-b8fc45725f9c' AND cast(answer as int64) in (9,10) THEN 'promotor'
              WHEN field_ref =  '1a3b8ab5-24d5-4b8b-a457-b8fc45725f9c' AND cast(answer as int64) in (7,8) THEN 'neutro'
              ELSE 'detractor' END AS respuesta,
              cast(submitted_at as date) fecha,
           FORMAT_DATE('%Y-%m-01', cast(submitted_at as date)) mes,
        FROM {{source('typeform','typeform_item_answer')}}   a -- `tenpo-external.typeform.typeform_item_answer`
        WHERE
            form_id = 'yXYjLgjX'
        ORDER BY
            item_id  desc
        )
    SELECT 
        nps.linea,
        nps.form_id,
        nps.pregunta,
        nps.respuesta,
        CASE 
           WHEN respuesta = 'promotor' THEN 1 
           WHEN respuesta = 'detractor' THEN -1 
           WHEN respuesta = 'neutro' THEN 0 ELSE null 
        END AS resp_recod,
        fecha,
        nps.mes
    FROM nps
    LEFT JOIN (
        SELECT
           ultima_fecha_envio_nps
      FROM {{ref('last_fecha_nps')}}
    ) ON 1 = 1    
    WHERE fecha <= ultima_fecha_envio_nps
    )

 ORDER BY mes DESC