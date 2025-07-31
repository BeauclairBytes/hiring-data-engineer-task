CREATE TABLE IF NOT EXISTS campaign_performance_daily
(
    campaign_id        UInt64,
    advertiser_id      UInt64,
    date               Date,
    day                UInt8,
    month              UInt8,
    year               UInt16,
    days_remaining     Int32,
    bid                Float64,
    daily_budget       Float64,
    daily_clicks       UInt64,
    daily_impressions  UInt64,
    ctr                Float64,
    dw_load_time       DateTime()
)
ENGINE = ReplacingMergeTree(dw_load_time)
PRIMARY KEY (campaign_id, advertiser_id, date)
ORDER BY (campaign_id, advertiser_id, date, dw_load_time)
