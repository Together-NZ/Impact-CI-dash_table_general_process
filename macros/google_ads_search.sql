{% macro google_ads_search(source_name, table_name) %}
SELECT
    SUM(conversions) AS conversions,
    date,
    SUM(media_cost) AS media_cost,
    SUM(clicks) AS clicks,
    segments_device AS device,
    SUM(impressions) AS impressions,
    campaign_name,
    campaign_id,
    publisher
FROM {{ source(source_name, table_name) }}
GROUP BY campaign_name, campaign_id, device, publisher, date
{% endmacro %}
