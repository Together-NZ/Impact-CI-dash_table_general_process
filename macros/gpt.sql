{% macro gpt(source_name, table_name) %}
    SELECT
        media_cost,
        impressions,
        clicks,
        creative_name,
        audience_name,
        ad_format,
        NULL AS ad_format_detail,
        CAST(0 AS INT64) AS video_completion,
        CAST(0 AS INT64) AS video_25_completion,
        CAST(0 AS INT64) AS video_50_completion,
        CAST(0 AS INT64) AS video_75_completion,
        CAST(0 AS INT64) AS video_views,
        campaign_name,
        publisher,
        campaign_descr,
        creative_descr,
        DATE(date) AS date,
        conversions,
        'ChatGPT' AS platform
    FROM {{ source(source_name, table_name) }}
{% endmacro %}
