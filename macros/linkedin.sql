{% macro linkedin(source_name,table_name) %}

    SELECT media_cost, impressions, clicks, creative_name, audience_name, ad_format, ad_format_detail, video_completion,video_25_completion,video_50_completion,video_75_completion,video_views,
            campaign_name, publisher, campaign_descr, creative_descr, date(date) as date,result AS conversions,
            'Linkedin' AS platform,
           SAFE_CAST(JSON_VALUE(TO_JSON_STRING(source_rows), '$.follows') AS INT64) AS follows
    FROM {{ source(source_name, table_name) }} AS source_rows
{% endmacro %}