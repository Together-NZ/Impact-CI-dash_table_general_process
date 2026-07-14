{% macro dash_table_general_process_funnel(funnels) %}
{#
  Same campaign / media_format logic as dash_table_general_process,
  but funnel labels are passed in (e.g. ['explore', 'compare', 'book', 'dream']).

  Usage:
    with_channel AS (...),
    {{ dash_table_general_process.dash_table_general_process_funnel(
         funnels=['explore', 'compare', 'book', 'dream']
       ) }}
#}
{% set funnel_sql %}[{% for f in funnels %}'{{ f | lower }}'{% if not loop.last %}, {% endif %}{% endfor %}]{% endset %}
campaign_base AS (
       SELECT *,
              CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_'))>=2 
              THEN SPLIT(campaign_name,'_')[1] 
              ELSE campaign_name
       END as campaign_name_raw
       FROM with_channel

),
campaign_name_selection_duplicate AS (
       SELECT COUNT(*) AS indicator,lower(campaign_name_raw) AS lower_campaign FROM (SELECT DISTINCT campaign_name_raw FROM campaign_base)
       GROUP BY LOWER(campaign_name_raw) HAVING COUNT(*)>1
),
duplicate_raw AS (
       SELECT distinct campaign_name_raw, ROW_NUMBER() OVER (PARTITION BY LOWER(campaign_name_raw) ORDER BY (campaign_name_raw)) as row_number from campaign_base cb join campaign_name_selection_duplicate cd
       ON LOWER(cb.campaign_name_raw) = LOWER(cd.lower_campaign) 
),
deduplicate_raw AS (
       select * from duplicate_raw where row_number = 1
)
SELECT camb.* EXCEPT(campaign_name_raw),
0 as metrics_value_per_conversion,
NULL AS segments_conversion_action,
NULL AS segments_conversion_action_category,
NULL AS segments_conversion_action_name,
NULL AS segments_conversion_attribution_event_type,
NULL AS segments_day_of_week,
NULL AS segments_month,
NULL AS segments_week,
NULL AS segments_quarter,
NULL AS segments_year,
NULL AS bidding_strategy_name,
NULL AS campaign_advertising_channel_sub_type,
NULL AS campaign_advertising_channel_type,
NULL AS campaign_bidding_strategy,
NULL AS campaign_bidding_strategy_type,
NULL AS campaign_budget_amount_micros,
NULL AS campaign_budget_explicitly_shared,
NULL AS campaign_budget_has_recommended_budget,
NULL AS campaign_budget_period,
NULL AS campaign_budget_recommended_budget_amount_micros,
NULL AS campaign_budget_total_amount_micros,
NULL AS campaign_campaign_budget,
NULL AS campaign_end_date,
NULL AS campaign_experiment_type,
NULL AS campaign_manual_cpc_enhanced_cpc_enabled,
NULL AS campaign_maximize_conversion_value_target_roas,
NULL AS campaign_percent_cpc_enhanced_cpc_enabled,
NULL AS campaign_serving_status,
NULL AS campaign_start_date,
NULL AS campaign_status,
NULL AS campaign_tracking_url_template,
NULL AS campaign_url_custom_parameters,
NULL AS campaign_id,
NULL AS customer_id,
NULL AS campaign_base_campaign,
NULL AS metrics_conversions,
NULL AS metrics_conversions_value,
NULL AS metrics_interaction_event_types,
NULL AS metrics_interactions,
NULL AS metrics_view_through_conversions,
NULL AS segments_ad_network_type,
NULL AS device,
NULL AS segments_slot,
NULL AS _LATEST_DATE,
NULL AS _DATA_DATE,
trim(CASE WHEN 
       lower(camb.campaign_name_raw) = lower(deduplicate_raw.campaign_name_raw) 
       
       THEN deduplicate_raw.campaign_name_raw
       ELSE camb.campaign_name_raw
END )AS campaign_name_selection,
CASE WHEN 
       EXISTS(SELECT 1 FROM UNNEST(SPLIT(creative_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(creative_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       WHEN  EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) in UNNEST(ARRAY['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt']))
       THEN  (SELECT X FROM UNNEST(SPLIT(campaign_name,'_') ) as X WHERE lower(X) IN UNNEST(['aud','dg','disp','native','pdooh','rmdisp','social','vid','vidod','yt'])
       LIMIT 1)
       WHEN EXISTS(SELECT 1 FROM UNNEST(SPLIT(creative_name,'_'))  as a
       WHERE lower(a) LIKE '%demand gen%')
       THEN 'DG'
       WHEN EXISTS(SELECT 1 FROM UNNEST(SPLIT(campaign_name,'_'))  as a
       WHERE lower(a) LIKE '%demand gen%')
       THEN 'DG'
       WHEN lower(campaign_name) like '%google%' and (lower(campaign_name) like '%native%' or lower(campaign_name) like '%demand gen%') then 'DEMAND GEN'
       else 'OTHER'
END as media_format,
CASE
  WHEN EXISTS (
    SELECT 1
    FROM UNNEST(SPLIT(campaign_name, '_')) AS a
    WHERE LOWER(a) IN UNNEST({{ funnel_sql }})
  )
  THEN (
    SELECT x
    FROM UNNEST(SPLIT(campaign_name, '_')) AS x
    WHERE LOWER(x) IN UNNEST({{ funnel_sql }})
    LIMIT 1
  )
  ELSE 'OTHER'
END AS funnel,
NULL AS sub_brands

 FROM campaign_base camb LEFT JOIN deduplicate_raw ON LOWER(deduplicate_raw.campaign_name_raw) = LOWER(camb.campaign_name_raw)

{% endmacro %}
