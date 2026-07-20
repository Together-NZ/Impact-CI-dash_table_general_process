{% macro dash_union_non_search(source_name, table_name,sub_brands) %}

(SELECT 
conversions AS conversions,
SAFE_CAST(campaign_name AS STRING) AS campaign_name,
SAFE_CAST(campaign_id AS INT64) AS campaign_id,
SAFE_CAST(device AS STRING) AS device,
SAFE_CAST(media_cost AS FLOAT64) AS media_cost,
SAFE_CAST(clicks AS INT64) AS clicks,
INITCAP(SAFE_CAST(funnel AS STRING)) AS funnel,
SAFE_CAST(impressions AS FLOAT64) AS impressions,
SAFE_CAST(date AS DATE) AS date,
SAFE_CAST(campaign_name_selection AS STRING) AS campaign_name_selection,
SAFE_CAST(publisher AS STRING) AS publisher,
SAFE_CAST(media_format AS STRING) AS media_format,
SAFE_CAST(channel AS STRING) AS channel,
SAFE_CAST(creative_name AS STRING) AS creative_name,
SAFE_CAST(ad_format AS STRING) AS ad_format,
SAFE_CAST(ad_format_detail AS STRING) AS ad_format_detail,
SAFE_CAST(audience_name AS STRING) AS audience_name,
SAFE_CAST(video_completion AS FLOAT64) AS video_completion,
SAFE_CAST(video_50_completion AS FLOAT64) AS video_50_completion,
SAFE_CAST(video_25_completion AS FLOAT64) AS video_25_completion,
SAFE_CAST(video_75_completion AS FLOAT64) AS video_75_completion,
SAFE_CAST(video_views AS INT64) AS video_views,
SAFE_CAST(campaign_descr AS STRING) AS campaign_descr,
SAFE_CAST(creative_descr AS STRING) AS creative_descr,
SAFE_CAST(platform AS STRING) AS platform,
CASE WHEN {{ sub_brands }} IS NOT NULL THEN {{ sub_brands }} ELSE NULL END AS sub_brands
  FROM {{ source(source_name, table_name) }} )
{% endmacro %}