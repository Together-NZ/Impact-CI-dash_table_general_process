{% macro snapchat(source_name,table_name) %}
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,null as conversions
    FROM {{ source(source_name, table_name) }}
{% endmacro %}