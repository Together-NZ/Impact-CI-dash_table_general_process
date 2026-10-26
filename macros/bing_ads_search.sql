{% macro bing_ads_search(source_name, table_name) %}
SELECT
    conversions,
    date,
    media_cost,
    clicks,
    device,
    impressions,
    campaign_name,
    campaign_id,
    publisher,
    'Bing Ads' AS platform,
           CAST(NULL AS INT64) AS follows
FROM {{ source(source_name, table_name) }}
{% endmacro %}
