{% macro dv360_standard(source_name,table_name) %}
    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion, video_25_completion as video_views,
           campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,SAFE_CAST(conversions as FLOAT64) as conversions
    
    FROM {{ source(source_name, table_name) }}  WHERE NOT (LOWER(campaign_name) LIKE '%yt%' OR LOWER(campaign_name) LIKE '%youtube%')
{% endmacro %}