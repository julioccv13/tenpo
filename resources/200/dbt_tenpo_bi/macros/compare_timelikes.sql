{%
    macro compare_timelikes(column_A, column_B, timelike_diff_function="date_diff", timelike_part_diff="day")
%} {{ timelike_diff_function }}({{ column_A }}, {{ column_B }}, {{ timelike_part_diff }}) {% endmacro %}