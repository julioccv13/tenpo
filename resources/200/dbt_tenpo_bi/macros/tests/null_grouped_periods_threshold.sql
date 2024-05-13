{% 
    test null_grouped_periods_threshold(
        model
        ,timelike_column="fecha_hora"
        ,group_column="event"
        ,timelike_expression="date"
        ,timelike_diff_function="date_diff"
        ,timelike_part_diff="day"
        ,timelike_min="'2020-06-01'"
        ,timelike_max="CURRENT_DATE()"
        ,extra_where_expression="1=1"
        ,threshold_expression='>=3') 
%}

with dates as (
    SELECT
    {{ timelike_expression }}({{ timelike_column }}) as timelike -- pasar a algo que ademas permita ver por zona horaria
    ,{{ group_column }}
    FROM {{ model }}
        WHERE ({{ timelike_expression }}({{ timelike_column }}) >= {{ timelike_min }} AND {{ timelike_expression }}({{ timelike_column }}) <= {{ timelike_max }})
        AND {{ extra_where_expression }}
    GROUP BY 1, 2
), lagged_dates as (
    select 
    *
    ,lag(timelike) over (partition by {{ group_column }} order by timelike) as lag_timelike
    from dates
), diffed_dates as (
    select
    *
    ,{{ compare_timelikes('timelike', 'lag_timelike', timelike_diff_function, timelike_part_diff) }} as timelike_diff
    from lagged_dates
)

select *
from diffed_dates
where timelike_diff {{ threshold_expression }}

{% endtest %}       