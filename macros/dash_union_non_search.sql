{% macro dash_union_non_search(source_name, table_name,sub_brands) %}

(WITH union_rows AS (
SELECT
conversions AS conversions,
SAFE_CAST(campaign_name AS STRING) AS campaign_name,
SAFE_CAST(campaign_id AS INT64) AS campaign_id,
SAFE_CAST(device AS STRING) AS device,
SAFE_CAST(media_cost AS FLOAT64) AS media_cost,
SAFE_CAST(clicks AS INT64) AS clicks,
INITCAP(SAFE_CAST(funnel AS STRING)) AS funnel,
SAFE_CAST(impressions AS FLOAT64) AS impressions,
SAFE_CAST(date AS DATE) AS date,
DATE_TRUNC(SAFE_CAST(date AS DATE), WEEK(MONDAY)) AS week,
DATE_TRUNC(SAFE_CAST(date AS DATE), MONTH) AS month,
DATE_TRUNC(SAFE_CAST(date AS DATE), YEAR) AS year,
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
CASE WHEN {{ sub_brands }} IS NOT NULL THEN {{ sub_brands }} ELSE NULL END AS sub_brands,
SAFE_CAST(JSON_VALUE(TO_JSON_STRING(source_rows), '$.follows') AS INT64) AS follows
  FROM {{ source(source_name, table_name) }} AS source_rows
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
  'CONTACT_0167_CON_MOBILE Q4_APR_2025_MOBILE_'
)
),
summerset_campaign_text AS (
    SELECT *,
        TRIM(REGEXP_REPLACE(
            TRANSLATE(LOWER(COALESCE(campaign_name, '')), 'āēīōū', 'aeiou'),
            r'[^a-z0-9]+', ' '
        )) AS summerset_campaign_match
    FROM union_rows
),
summerset_funnel AS (
    SELECT * EXCEPT(funnel, summerset_campaign_match),
        CASE
            WHEN campaign_name IN (
                'SUM244 |  | National | 14 | NZ | Memory Care | Low Priority Locations',
                'SUM244 |  | National | 14 | NZ | Memory Care | Priority Locations',
                'SUM244 |  | National | 14 | NZ | Search Remarketing'
            ) THEN 'Special'
            WHEN NOT REGEXP_CONTAINS(LOWER(COALESCE(campaign_name, '')), r'(^|[^a-z0-9])sum([0-9]|[^a-z0-9]|$)|summerset')
            THEN funnel
            WHEN summerset_campaign_match LIKE '%summerset mount denby%'
              OR summerset_campaign_match LIKE '%whangarei%'
            THEN 'Northland'
            WHEN summerset_campaign_match LIKE '%summerset st johns%'
              OR summerset_campaign_match LIKE '%st johns%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset half moon bay%'
              OR summerset_campaign_match LIKE '%half moon bay%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset milldale%'
              OR summerset_campaign_match LIKE '%milldale%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset at heritage park%'
              OR summerset_campaign_match LIKE '%ellerslie%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset by the park%'
              OR summerset_campaign_match LIKE '%manukau%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset at monterey park%'
              OR summerset_campaign_match LIKE '%hobsonville%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset falls%'
              OR summerset_campaign_match LIKE '%warkworth%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset at karaka%'
              OR summerset_campaign_match LIKE '%karaka%'
            THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%summerset cambridge%'
              OR summerset_campaign_match LIKE '%cambridge%'
            THEN 'Waikato'
            WHEN summerset_campaign_match LIKE '%summerset down the lane%'
              OR summerset_campaign_match LIKE '%hamilton fitzroy%'
            THEN 'Waikato'
            WHEN summerset_campaign_match LIKE '%summerset rototuna%'
              OR summerset_campaign_match LIKE '%hamilton rototuna%'
            THEN 'Waikato'
            WHEN summerset_campaign_match LIKE '%summerset by the lake%'
              OR summerset_campaign_match LIKE '%taupo%'
            THEN 'Waikato'
            WHEN summerset_campaign_match LIKE '%summerset by the dunes%'
              OR summerset_campaign_match LIKE '%papamoa%'
            THEN 'Bay of Plenty'
            WHEN summerset_campaign_match LIKE '%summerset by the sea%'
              OR summerset_campaign_match LIKE '%katikati%'
            THEN 'Bay of Plenty'
            WHEN summerset_campaign_match LIKE '%summerset palms%'
              OR summerset_campaign_match LIKE '%napier te awa%'
            THEN 'Hawkes Bay'
            WHEN summerset_campaign_match LIKE '%summerset in the orchard%'
              OR summerset_campaign_match LIKE '%hastings%'
            THEN 'Hawkes Bay'
            WHEN summerset_campaign_match LIKE '%summerset in the vines%'
              OR summerset_campaign_match LIKE '%havelock north%'
            THEN 'Hawkes Bay'
            WHEN summerset_campaign_match LIKE '%summerset in the bay%'
              OR summerset_campaign_match LIKE '%napier greenmeadows%'
            THEN 'Hawkes Bay'
            WHEN summerset_campaign_match LIKE '%summerset at pohutukawa place%'
              OR summerset_campaign_match LIKE '%new plymouth bell block%'
            THEN 'Taranaki'
            WHEN summerset_campaign_match LIKE '%summerset mountain view%'
              OR summerset_campaign_match LIKE '%new plymouth vogeltown%'
            THEN 'Taranaki'
            WHEN summerset_campaign_match LIKE '%summerset kelvin grove%'
              OR summerset_campaign_match LIKE '%palmerston north%'
            THEN 'Manawatu-Whanganui'
            WHEN summerset_campaign_match LIKE '%summerset by the ranges%'
              OR summerset_campaign_match LIKE '%levin%'
            THEN 'Manawatu-Whanganui'
            WHEN summerset_campaign_match LIKE '%summerset on summerhill%'
              OR summerset_campaign_match LIKE '%palmerston north%'
            THEN 'Manawatu-Whanganui'
            WHEN summerset_campaign_match LIKE '%summerset in the river city%'
              OR summerset_campaign_match LIKE '%whanganui%'
            THEN 'Manawatu-Whanganui'
            WHEN summerset_campaign_match LIKE '%summerset boulcott%'
              OR summerset_campaign_match LIKE '%lower hutt%'
            THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%summerset waikanae%'
              OR summerset_campaign_match LIKE '%waikanae%'
            THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%summerset on the landing%'
              OR summerset_campaign_match LIKE '%kenepuru%'
            THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%summerset at aotea%'
              OR summerset_campaign_match LIKE '%porirua%'
            THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%summerset on the coast%'
              OR summerset_campaign_match LIKE '%paraparaumu%'
            THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%summerset at the course%'
              OR summerset_campaign_match LIKE '%upper hutt%'
            THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%summerset blenheim%'
              OR summerset_campaign_match LIKE '%blenheim%'
            THEN 'Marlborough'
            WHEN summerset_campaign_match LIKE '%summerset richmond ranges%'
              OR summerset_campaign_match LIKE '%richmond%'
            THEN 'Nelson-Tasman'
            WHEN summerset_campaign_match LIKE '%summerset in the sun%'
              OR summerset_campaign_match LIKE '%nelson stoke%'
            THEN 'Nelson-Tasman'
            WHEN summerset_campaign_match LIKE '%summerset prebbleton%'
              OR summerset_campaign_match LIKE '%prebbleton%'
            THEN 'Canterbury'
            WHEN summerset_campaign_match LIKE '%summerset rangiora%'
              OR summerset_campaign_match LIKE '%rangiora%'
            THEN 'Canterbury'
            WHEN summerset_campaign_match LIKE '%summerset at avonhead%'
              OR summerset_campaign_match LIKE '%christchurch%'
            THEN 'Canterbury'
            WHEN summerset_campaign_match LIKE '%summerset on cavendish%'
              OR summerset_campaign_match LIKE '%christchurch casebrook%'
            THEN 'Canterbury'
            WHEN summerset_campaign_match LIKE '%summerset at wigram%'
              OR summerset_campaign_match LIKE '%christchurch%'
            THEN 'Canterbury'
            WHEN summerset_campaign_match LIKE '%summerset at bishopscourt%'
              OR summerset_campaign_match LIKE '%dunedin%'
            THEN 'Otago'
            WHEN TRIM(REGEXP_REPLACE(REPLACE(campaign_name, CHR(160), ' '), r'\s+', ' ')) IN (
                'SUM327 | Search | Oakleigh South | Generic',
                'SUM327 | Search | Chirnside Park | Brand',
                'SUM327 | Search | Chirnside Park | Generic',
                'SUM327 | Search | Cranbourne North | Brand',
                'SUM327 | Search | Cranbourne North | Generic',
                'SUM327 | Search | Oakleigh South | Brand'
            ) THEN 'Victoria'
            WHEN summerset_campaign_match LIKE '%national%' THEN 'National'
            WHEN summerset_campaign_match LIKE '%northland%' THEN 'Northland'
            WHEN summerset_campaign_match LIKE '%auckland%' THEN 'Auckland'
            WHEN summerset_campaign_match LIKE '%bay of plenty%' THEN 'Bay of Plenty'
            WHEN summerset_campaign_match LIKE '%waikato%' THEN 'Waikato'
            WHEN summerset_campaign_match LIKE '%hawkes bay%' OR summerset_campaign_match LIKE '%hawke s bay%' THEN 'Hawkes Bay'
            WHEN summerset_campaign_match LIKE '%gisborne%' THEN 'Gisborne'
            WHEN summerset_campaign_match LIKE '%taranaki%' THEN 'Taranaki'
            WHEN summerset_campaign_match LIKE '%manawatu whanganui%' THEN 'Manawatu-Whanganui'
            WHEN summerset_campaign_match LIKE '%wellington%' THEN 'Wellington'
            WHEN summerset_campaign_match LIKE '%marlborough%' THEN 'Marlborough'
            WHEN summerset_campaign_match LIKE '%nelson tasman%' THEN 'Nelson-Tasman'
            WHEN summerset_campaign_match LIKE '%canterbury%' THEN 'Canterbury'
            WHEN summerset_campaign_match LIKE '%otago%' THEN 'Otago'
            ELSE 'Others'
        END AS funnel
    FROM summerset_campaign_text
)
SELECT * FROM summerset_funnel
)
{% endmacro %}