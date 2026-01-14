-- KPI report for the last full week (Monday to Sunday).
-- Includes actual start and end dates of the period for clarity and filtering.

SELECT
    CurrentUtcDate() - CAST(Datetime::GetDayOfWeek(CurrentUtcDate()) + 6 AS Interval) AS period_start,
    CurrentUtcDate() - CAST(Datetime::GetDayOfWeek(CurrentUtcDate()) AS Interval) AS period_end,

    SUM(total_events) AS total_clicks,
    COUNT(*) AS active_users,
    AVG(CAST(mobile_clicks AS DOUBLE)) AS avg_mobile_clicks_per_user,
    SUM(downloads) AS total_downloads,
    Math::Round(
        CAST(SUM(downloads) AS DOUBLE) * 100 / SUM(total_events),
        2
    ) AS download_ctr_pct,
    AVG(CAST(unique_pages_visited AS DOUBLE)) AS avg_pages_per_user,
    SUM(CASE WHEN user_agent_id_unique_count > 1 THEN 1 ELSE 0 END) AS multi_browser_users
FROM {{ ref('agg_user_activity_daily') }}
WHERE
    event_date >= CurrentUtcDate() - CAST(Datetime::GetDayOfWeek(CurrentUtcDate()) + 6 AS Interval)
    AND event_date <= CurrentUtcDate() - CAST(Datetime::GetDayOfWeek(CurrentUtcDate()) AS Interval)


