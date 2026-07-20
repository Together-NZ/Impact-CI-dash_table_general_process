{% macro reddit(source_name,table_name) %}
       select media_cost,impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, 
        CAST(video_completion AS INT64) AS video_completion,
        CAST(video_25_completion AS INT64) AS video_25_completion,
        CAST(video_50_completion AS INT64) AS video_50_completion,
        CAST(video_75_completion AS INT64) AS video_75_completion,
        CAST(video_25_completion AS INT64) AS video_views,
    campaign_name,  publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
FROM {{ source(source_name, table_name) }}
{% endmacro %}