{% macro hash_sensible_data(variable) %}

TO_BASE64(SHA256("{{ env_var('SENSIBLE_SHA256_PEPPER_SECRET') }}" || {{ variable }}))

{% endmacro %}
