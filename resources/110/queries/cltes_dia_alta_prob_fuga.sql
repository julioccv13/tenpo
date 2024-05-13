select distinct
'i' as type,
user as identity
from `{project_source_5}.analytics_models.early_churn_predict_*`
where date(fecha_ob) = date_sub(current_date(), interval 32 day)
and probabilidad_fuga>=0.4
