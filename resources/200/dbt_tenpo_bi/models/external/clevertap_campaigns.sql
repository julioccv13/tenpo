{{ 
  config(
    materialized='table',
  ) 
}}


WITH campaigns AS 
(SELECT 
    DISTINCT
    campaign,
    start_date,
    campaign_id,
    channel,
    title,
    message,
    campaign_url,
    sum(estimated_reach) estimated_reach,
    sum(total_sent_users) total_sent_users,
    sum(total_viewed_users) total_viewed_users,
    sum(total_clickedusers) total_clicked_users,
    sum(CAST(conversions AS INT64)) conversions,
    sum(app_uninstalled_android) app_uninstalled_android,
    sum(app_uninstalled_ios)	app_uninstalled_ios,	
    sum(CAST(erros AS INT64)) errors,
    report_date,
    ROW_NUMBER() OVER ( PARTITION BY campaign ORDER BY report_date DESC) as row_no
--FROM `tenpo-external.clevertap.campaigns`
FROM {{ source('clevertap', 'campaigns') }}
GROUP BY 1,2,3,4,5,6,7,report_date
ORDER BY  campaign_id, report_date DESC) 

SELECT * EXCEPT (row_no) FROM campaigns
WHERE row_no = 1