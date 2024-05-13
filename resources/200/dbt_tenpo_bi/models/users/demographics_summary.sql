{{ config(materialized='table') }}
SELECT 
    Fecha_Fin_Analisis_DT
    ,count(distinct tenpo_uuid) AS count
    ,count(distinct case when (activo_mes_app is true) then tenpo_uuid else null end) cuenta_mau
    ,avg(case when gender='female' then 1 else 0 end) as pct_mujeres_ob
    ,avg(case when nationality not like '%Chile%' then 1 else 0 end) as pct_extranjeros
    -->>>>>MAUS<<<<<--
    ,count(distinct case when (gender = 'female' AND activo_mes_app is true) then tenpo_uuid else null end) cuenta_mujer_mau
    ,count(distinct case when (gender = 'male' AND activo_mes_app is true ) then tenpo_uuid else null end) cuenta_hombre_mau
    ,avg(case when gender='female' AND activo_mes_app then 1 else 0 end) as pct_mujeres_maus
    ,avg(case when gender='male' AND activo_mes_app then 1 else 0 end) as pct_hombres_maus
    -->>>>>EDAD<<<<<--
    ,avg(case when gender='female' then b.age else null end) as avg_edad_mujeres
    ,fhoffa.x.median(array_agg(case when gender='female' then b.age else null end IGNORE NULLS)) as median_edad_mujeres
    ,avg(case when gender='male' then b.age else null end) as avg_edad_hombres
    ,fhoffa.x.median(array_agg(case when gender='male' then b.age else null end IGNORE NULLS)) as median_edad_hombres
    -->>>>>TENENCIA<<<<<--
    ,avg(case when gender='female' then b.tenpo_tenencia_productos else null end) as avg_tenencia_mujeres
    ,fhoffa.x.median(array_agg(case when gender='female' then b.tenpo_tenencia_productos else null end IGNORE NULLS)) as median_tenencia_mujeres
    ,avg(case when gender='male' then b.tenpo_tenencia_productos else null end) as avg_tenencia_hombres
    ,fhoffa.x.median(array_agg(case when gender='male' then b.tenpo_tenencia_productos else null end IGNORE NULLS)) as median_tenencia_hombres
FROM {{source('tablones_analisis','tablon_monthly_vectores_usuarios')}} b 
WHERE 
    b.ob_completed_at is not null and state in (4,7,8,21,22)
GROUP BY  1
ORDER BY 2 DESC