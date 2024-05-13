{{ config(materialized='table') }}
with preguntas as (
  select 'AMBe5TZFwGXV' as field_id,'¿En una escala del 0 al 10, qué tanto recomendarías a un amigo, compañero de trabajo o familiar Recarga Fácil?' as field_text,'top_ups' as form_text union all
select 'QwHpI7bINQkt','¿Qué mejorarías del servicio de recarga fácil?','top_ups' union all
select 'cIXCSmV5KpC3','¿Cuánto esfuerzo significo para ti lograr tu recarga? En una escala de 1 a 5, dónde 1 es mucho esfuerzo y 5 es nada de esfuerzo','top_ups' union all
select 'wIYEUgfHwVGB','¿Después de cuántos intentos lograste una recarga exitosa?','top_ups' union all
select 'Yl03oiikJCmH','¿Qué mejorarías del servicio de agregar dólares a PayPal?','paypal_abono' union all
select 'w1cp1PSIvgrf','¿En una escala del 0 al 10, qué tanto recomendarías a un amigo, compañero de trabajo o familiar el *servicio de agregar dólares a PayPal*?','paypal_abono' union all
select 'f4koEWdpsDff','¿Qué mejorarías del servicio de retiro de dólares PayPal?','paypal_retiro' union all
select 'vSEYTCOPpH5e','¿En una escala del 0 al 10, qué tanto recomendarías a un amigo, compañero de trabajo o familiar el *servicio de retiro de dólares PayPal*?','paypal_retiro'
)
SELECT distinct 
  form_text, 
  item_id,
  MAX(if(field_id in ('AMBe5TZFwGXV','w1cp1PSIvgrf','vSEYTCOPpH5e'),answer,null)) as nps,
  CASE 
    WHEN CAST(MAX(if(field_id in ('AMBe5TZFwGXV','w1cp1PSIvgrf','vSEYTCOPpH5e'),answer,null)) AS INT64) > 8 THEN 1
    WHEN CAST(MAX(if(field_id in ('AMBe5TZFwGXV','w1cp1PSIvgrf','vSEYTCOPpH5e'),answer,null)) AS INT64) > 6 THEN 0
    WHEN CAST(MAX(if(field_id in ('AMBe5TZFwGXV','w1cp1PSIvgrf','vSEYTCOPpH5e'),answer,null)) AS INT64) < 7 THEN -1
     END as nps_score,
  MAX(if(field_id in ('Yl03oiikJCmH','f4koEWdpsDff','QwHpI7bINQkt'),answer,null)) as to_improve,
  MAX(if(field_id in ('cIXCSmV5KpC3'),answer,null)) as effort_to_topup,
  MAX(if(field_id in ('wIYEUgfHwVGB'),answer,null)) as retries_topup,
  submitted_at 
FROM {{source('typeform','typeform_item_answer')}}
JOIN preguntas using (field_id)
where form_id in ('FGW8nJ','ox9et1','ngmGeQ') 
group by 1,2,submitted_at
order by 2