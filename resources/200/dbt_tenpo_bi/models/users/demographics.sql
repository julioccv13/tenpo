{{ config(tags=["hourly", "bi"], materialized='table') }}

 WITH 
  pub_obj as (
    SELECT
      u.id id_usuario 
      ,s.state 
      ,{{ hash_sensible_data('u.first_name') }}  nombre
      ,DATE(u.ob_completed_at , "America/Santiago") timestamp_ob
      ,DATE(u.ob_completed_at , "America/Santiago") fecha_ob
      ,IF(EXTRACT(DAYOFYEAR FROM CURRENT_DATE) < EXTRACT(DAYOFYEAR FROM DATE(date_of_birth , "America/Santiago"))
      ,DATE_DIFF(CURRENT_DATE, DATE(date_of_birth , "America/Santiago") , YEAR) - 1
      ,DATE_DIFF(CURRENT_DATE, DATE(date_of_birth , "America/Santiago")  , YEAR)) AS edad
      ,u.nationality nacionalidad
      ,{{target.schema}}.accent2latin(UPPER(coalesce(re.nombre_region,rr.region_rename,u.region,r.region_acronym,cast(u.region_code as string)))) sigla_region
      ,INITCAP(coalesce(r.region_name,u.commune)) comuna
      ,g.gender
      ,uas.last_ndd_service_bm as source
      ,uas.last_serv_paypal_bm as serv_paypal
      ,services
      ,actividad_bm
      ,COALESCE(last_platform, 'unknown') dispositivo
      ,COALESCE(SPLIT(SPLIT(lower(last_device_type) , '-')[OFFSET(0)], ",")[OFFSET(0)] , 'unknown') modelo
      ,CASE 
      WHEN cliente = 1 and churn = 1 THEN 'Desinstala'
      WHEN cliente = 0 and churn = 1  THEN 'Cuenta Cerrada'
      ELSE  'Cliente Ready'
      END as estado_churn
    FROM   {{ref('users_tenpo')}} u --{{ref('users_tenpo')}} u
    LEFT JOIN {{ source('tenpo_users', 'states_user_dict') }}  s on u.state =  s.state_id --`tenpo-airflow-prod.users.states_user_dict` s on u.state =  s.state_id
    LEFT JOIN {{ source('aux_table', 'regions') }} r USING (region_code) --`tenpo-bi.aux_table.regions` r USING (region_code)
    LEFT JOIN {{ source('aux_table', 'name_genders') }} g USING(id) --`tenpo-bi.aux_table.name_genders` g  USING(id)
    LEFT JOIN {{ref('platform')}} d  ON d.user =  u.id --`tenpo-bi-prod.activity.platform`   d  ON d.user =  u.id
    LEFT JOIN {{ref('device_type')}} dt  ON dt.user =  u.id
    LEFT JOIN {{ref('users_allservices')}} uas  ON (uas.id=u.id)  --`tenpo-bi-prod.users.users_allservices` uas  ON (uas.id=u.id) 
    LEFT JOIN {{ ref('regiones') }} re on (UPPER(r.region_acronym)=UPPER(re.sigla_region))
    LEFT JOIN {{ ref('region_rename') }} rr on (UPPER(u.region)=UPPER(rr.region_name))
    WHERE
      u.status_onboarding = 'completo'
    QUALIFY 
      row_number() over (partition by u.id order by updated_at desc) = 1
      ),
      
  actividad as (
    SELECT DISTINCT
      user,
      LAST_VALUE(uniq_flujos_origen) OVER  (PARTITION BY user ORDER BY Fecha_Fin_Analisis_DT ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) uniq_flujos_origen
    FROM {{ source('productos_tenpo', 'tenencia_productos_tenpo') }}  --`tenpo-bi.productos_tenpo.tenencia_productos_tenpo` 
    WHERE uniq_flujos_origen > 0
    ),
    
  referente as (
    SELECT distinct
      referrer user
    FROM {{ref('parejas_iyg')}} --`tenpo-bi-prod.referral_program.parejas_iyg` 
    ),
    
  invitados as (
    SELECT distinct
      user 
    FROM {{ref('parejas_iyg')}} --`tenpo-bi-prod.referral_program.parejas_iyg` 
    WHERE 
      user is not null
    ),

  lineas as (
    SELECT distinct
        user,
        logical_or(linea="utility_payments") as utility_payments,
        logical_or(linea="top_ups") as top_ups,
        logical_or(linea="paypal") as paypal,
        logical_or(linea="crossborder") as crossborder,
        logical_or(linea="mastercard") as mastercard,
        logical_or(linea="mastercard_physical") as mastercard_physical,
        logical_or(linea like "%savings%") as savings,
        logical_or(linea like "%p2p%") as p2p,
        #TO_JSON_STRING(`tenpo-bi.aux_table.FILTER_WARDS_DISTINCT`(array_agg(linea) over (partition by user order by linea rows between unbounded preceding and unbounded following))) lineas,
      FROM `tenpo-bi-prod.users.clientes_tenpo_datos_transaccionales`
    group by user
    ),

    mau_type as (

    SELECT 
        user,
        mau_type,
    FROM `tenpo-bi.temp.query_mau_detalle`
    WHERE true
    QUALIFY row_number() OVER (PARTITION BY  user ORDER BY mes DESC) = 1
    ),

    bank_characterization AS (

    SELECT 
        DISTINCT
        user,
        caracterizacion
    FROM `tenpo-bi-prod.bancarization.bank_characterization`  -- 0. Sin cashin reconocible (o solo fisico)
    ),
    
  summary as (
    SELECT DISTINCT
      u.*
      ,l.*
      ,CASE WHEN uniq_flujos_origen > 0 THEN 'Con actividad' ELSE 'Sin actividad' END AS tipo_actividad
      ,CASE WHEN r.user is not null and i.user is not null then 'Referente y Referido'
       WHEN r.user is null and i.user is not null then 'Referido'
       WHEN r.user is not null and i.user is null then 'Referente'
       WHEN r.user is null and i.user is null then 'No está en el programa'
       ELSE null
       END as estado_iyg,
       IFNULL(mau_type,"no mau") mau_type,
       IFNULL(caracterizacion,"0. Sin cashin reconocible (o solo fisico)") caracterizacion     
    FROM pub_obj u   --{{ ref('users_tenpo') }}  u
     LEFT JOIN actividad a ON id_usuario = a.user
     LEFT JOIN invitados i ON id_usuario = i.user
     LEFT JOIN referente r ON id_usuario = r.user
     LEFT JOIN lineas l ON id_usuario = l.user
     LEFT JOIN mau_type m ON id_usuario = m.user 
     LEFT JOIN bank_characterization b ON id_usuario = b.user
     )
      
SELECT
  *
FROM summary 