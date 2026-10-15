{% macro publisher_channel(input_relation='dash_table', mapping_relation='`together-internal.channel.publisher_channel`', exclude_columns=[]) %}
{# Keep unmapped rows and publisher names. Legacy inputs may lack platform. #}
SELECT
    dt.* EXCEPT (publisher{% for column in exclude_columns %}, {{ column }}{% endfor %}),
    COALESCE(dc.publisher, dt.publisher) AS publisher,
    CASE
        WHEN dc.channel IS NOT NULL THEN dc.channel
        WHEN JSON_VALUE(TO_JSON_STRING(dt), '$.platform') = 'CM360' THEN 'Paid Display'
        ELSE 'Other'
    END AS channel
FROM {{ input_relation }} AS dt
LEFT JOIN {{ mapping_relation }} AS dc
    ON LOWER(TRIM(dt.publisher)) = LOWER(TRIM(dc.publisher))
{% endmacro %}
