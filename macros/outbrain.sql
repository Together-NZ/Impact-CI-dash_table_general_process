{% macro outbrain(source_name, table_name) %}
    SELECT
        SAFE_CAST(media_cost AS FLOAT64) AS media_cost,
        SAFE_CAST(impressions AS INT64) AS impressions,
        SAFE_CAST(clicks AS INT64) AS clicks,
        creative_name,
        audience_name,
        ad_format,
        ad_format_detail,
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
        SAFE_CAST(conversions AS FLOAT64) AS conversions,
        'Outbrain' AS platform
    FROM {{ source(source_name, table_name) }}
{% endmacro %}