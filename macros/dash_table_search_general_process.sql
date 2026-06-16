{% macro dash_table_search_general_process(funnel) %}
SELECT *,
CASE WHEN 
 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       WHEN LOWER(campaign_name) LIKE '%pmax%' OR LOWER(campaign_name) LIKE '%performance max%' OR LOWER(campaign_name) LIKE '%performance-max%' THEN 'PERFORMANCE MAX'
       WHEN LOWER(campaign_name) LIKE '%search%' OR LOWER(campaign_name) LIKE '%search%' OR LOWER(campaign_name) LIKE '%search%' THEN 'SEARCH'
       ELSE 'OTHER'
       END as media_format,
CASE WHEN lower(publisher) = 'demand gen' THEN 'Demand Gen'
ELSE 'Paid Search' END as channel,
'{{ funnel }}' as funnel,

CASE WHEN LOWER(publisher) != 'demand gen' THEN campaign_name 
ELSE 
CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=2 
              THEN SPLIT(campaign_name,'_')[1] 
              ELSE campaign_name
END END AS campaign_name_selection,

NULL AS creative_name,
NULL AS ad_format, 
NULL AS ad_format_detail,
NULL AS audience_name,
NULL AS video_completion,
NULL AS video_50_completion,
NULL AS video_25_completion,
NULL AS video_75_completion,
null as video_views,
null as campaign_descr,

null as creative_descr
from dash_table
{% endmacro %}