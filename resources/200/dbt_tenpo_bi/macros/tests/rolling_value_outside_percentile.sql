{% 
    test rolling_value_outside_percentile(
        model
        ,timelike_expression="date(fecha_hora)"
        ,value_expression="count(distinct identity)"
        ,lower_bound_percentile=25
        ,lower_bound_expression='lower_bound_percentile - 1.5*iqr'
        ,upper_bound_percentile=75
        ,upper_bound_expression='upper_bound_percentile + 1.5*iqr'
        ,timelike_min="'2020-06-01'"
        ,timelike_max="CURRENT_DATE()"
        ,extra_where_expression="1=1"
        ,min_samples=25) 
%}

with data as (
    SELECT
    {{ timelike_expression }} as timelike -- pasar a algo que ademas permita ver por zona horaria
    ,{{ value_expression }} as value
    FROM {{ model }}
        WHERE ({{ timelike_expression }} >= {{ timelike_min }} AND {{ timelike_expression }} <= {{ timelike_max }})
        AND {{ extra_where_expression }}
    GROUP BY 1
), array_data as (
    select 
    *
    ,array_agg(value) over (order by timelike rows between unbounded preceding and 1 preceding) as value_array
    ,count(value) over (order by timelike rows between unbounded preceding and 1 preceding) as samples
    from data
), quantile_data as (
    select 
    *
    ,(SELECT APPROX_QUANTILES(x, 100) AS approx_quantiles
    FROM UNNEST(value_array) AS x) as quantiles
    from array_data
), selected_quantile_data as (
    select 
    * except(quantiles)
    ,{{ lower_bound_percentile }} as lower_percentile
    ,{{ upper_bound_percentile }} as upper_percentile
    ,quantiles[offset({{ lower_bound_percentile }})] as lower_bound_percentile
    ,quantiles[offset({{ upper_bound_percentile }})] as upper_bound_percentile
    ,quantiles[offset({{ upper_bound_percentile }})] - quantiles[offset({{ lower_bound_percentile }})] as iqr
    from quantile_data
), boundaries_data as (
    select
    * 
    ,'{{ lower_bound_expression }}' as lower_bound_expression
    ,'{{ upper_bound_expression }}' as upper_bound_expression
    ,{{ lower_bound_expression }} as lower_bound
    ,{{ upper_bound_expression }} as upper_bound
    from selected_quantile_data 
)

select *
from boundaries_data 
where value < lower_bound or value > upper_bound 
and samples > {{ min_samples }}


{% endtest %}       