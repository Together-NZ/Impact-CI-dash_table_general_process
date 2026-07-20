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
    publisher
FROM {{ source(source_name, table_name) }}
{% endmacro %}
