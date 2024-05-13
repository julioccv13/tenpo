{{ 
  config(
    tags=["hourly", "ccam","savings","datamart"],
    materialized='table', 
    project=env_var('DBT_PROJECT26', 'tenpo-datalake-sandbox')
  ) 
}}

with 
user_risk_profile as (
  select distinct
    id,
    user_id,
    min(created_at) over (partition by user_id) as suggested_risk_profile_created_at,
    max(created_at) over (partition by user_id) as lastest_chosen_risk_profile_created_at,
    array_to_string(array_agg(chosen_risk_profile) over (partition by user_id order by created_at),",") as arr_chosen_risk_profiles,
    suggested_risk_profile,
    chosen_risk_profile,
  from (select distinct * from {{source('onboarding_savings','user_risk_profile')}})
  where true
  qualify row_number() over (partition by user_id order by created_at desc) = 1
),
preguntas_perfil as (
  WITH answers AS
    (
      SELECT 
        *,
        JSON_EXTRACT_ARRAY(replace(answers,"u'","'"),"$") as json
      from {{source('aux_table','id_preguntas_perfil_inversionista')}}
    )
  SELECT  distinct
    * except (json,json_answers,answers,product,created_at,updated_at), 
    json_extract(json_answers,"$.id") as answer_id,
    json_extract(json_answers,"$.response") as response,
  FROM answers
  CROSS JOIN UNNEST(answers.json) AS json_answers
),
data_user as (
  select distinct
    *
  from {{ref('users_savings')}}
)
select distinct
  t3.user_id,
  t4.tributary_identifier,
  t3.suggested_risk_profile_created_at,
  t3.lastest_chosen_risk_profile_created_at,
  t3.arr_chosen_risk_profiles,
  t2.order as n_question,
  t2.question,
  t1.answer_code as id_response,
  t2.response,
  t3.suggested_risk_profile,
  t3.chosen_risk_profile,
from {{source('onboarding_savings','user_risk_profile_answer')}} as t1
  join preguntas_perfil  as t2 on (t1.question_id=t2.id and t2.answer_id=t1.answer_code)
  join user_risk_profile as t3 on (t3.id = t1.user_risk_profile_id) 
  join data_user as t4 on (t3.user_id = t4.id)
order by 1,2,3, n_question