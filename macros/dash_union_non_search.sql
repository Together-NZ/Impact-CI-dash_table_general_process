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
UPPER(SAFE_CAST(media_format AS STRING)) AS media_format,
SAFE_CAST(channel AS STRING) AS channel,
SAFE_CAST(creative_name AS STRING) AS creative_name,
SAFE_CAST(ad_format AS STRING) AS ad_format1,
SAFE_CAST(ad_format_detail AS STRING) AS ad_format,
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
  FROM {{ source(source_name, table_name) }} 
WHERE LOWER(SAFE_CAST(campaign_name AS STRING)) NOT LIKE '%dnu%' AND campaign_name NOT IN (
  'CON030 - MULTI - Contact - Broadband - Product - Broadband Bundle Hero - 040520 - 150620',
  'CONTACT_0000_CON_TIKTOK CONTENT_JAN_2025_ELECTRICITY_',
  'CONTACT_0122_CON_RETAIL Q3 FLEX_JAN MAR_2024_RETAIL_FLEX',
  'CONTACT_0143_AWA_GOOD PLANS Q1_JUL_2024_ELECTRICITY_',
  'CONTACT_0156_CON_GOOD PLANS Q3_JAN_2025_ELECTRICITY_',
  'CONTACT_0158_CON_BROADBAND Q3_JAN_2025_BROADBAND_',
  'CONTACT_0158_CON_MOBILE Q3_JAN_2025_MOBILE_',
  'CONTACT_0165_CON_GOOD PLANS Q4_APR_2025_ELECTRICITY_',
  'CONTACT_0166_CON_MOBILE Q4_APR_2025_MOBILE_',
  'CONTACT_0167_CON_BROADBAND Q4_APR_2025_BROADBAND_',
  'CONTACT_0167_CON_MOBILE Q4_APR_2025_MOBILE_',
))

{% endmacro %}