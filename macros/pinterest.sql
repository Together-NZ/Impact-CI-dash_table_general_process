{% macro pinterest(source_name, table_name) %}
    SELECT
        SAFE_CAST(media_cost AS FLOAT64) AS media_cost,
        SAFE_CAST(impressions AS INT64) AS impressions,
        SAFE_CAST(clicks AS INT64) AS clicks,
        creative_name,
        audience_name,
        ad_format,
        ad_format_detail,
        video_completion,
        video_25_completion,
        video_50_completion,
        video_75_completion,
        video_views,
        campaign_name,
        publisher,
        campaign_descr,
        creative_descr,
        DATE(date) AS date,
        SAFE_CAST(conversions AS INT64) AS conversions,
        'Pinterest' AS platform
    FROM {{ source(source_name, table_name) }}
{% endmacro %}