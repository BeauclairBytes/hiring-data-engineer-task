-- Calculates CRT per campaign
SELECT campaign_id, date,  SUM(ctr) as ctr
FROM campaign_performance_daily
GROUP BY 1,2;


-- Calulates daily impressions and clicks
SELECT campaign_id, date,  SUM(daily_impressions) as daily_impressions, SUM(daily_clicks) as daily_clicks
FROM campaign_performance_daily
GROUP BY 1,2;


-- Get impressions clicks together with remaining days and daily budget:
SELECT campaign_id, date,  days_remaining, daily_budget,  daily_impressions, daily_clicks
FROM campaign_performance_daily
ORDER BY campaign_id, date
;