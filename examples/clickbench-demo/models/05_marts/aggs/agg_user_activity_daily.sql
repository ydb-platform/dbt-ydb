-- daily user activity with atomic, non-lossy metrics
-- Ready to build engagement, attribution, device, and conversion logic
SELECT
    user_id,
    event_date,

    -- Event counts
    COUNT(*) AS total_events,

    -- Device breakdown
    SUM(is_mobile) AS mobile_clicks,
    CAST(COUNT(*) AS Int64) - SUM(is_mobile) AS desktop_clicks,

    -- Action types
    SUM(CASE WHEN is_link_click = 1 THEN 1 ELSE 0 END) AS link_clicks,
    SUM(CASE WHEN is_download = 1 THEN 1 ELSE 0 END) AS downloads,
    SUM(CASE WHEN is_not_bounce = 1 THEN 1 ELSE 0 END) AS not_bounces,
    SUM(CASE WHEN is_not_bounce = 0 THEN 1 ELSE 0 END) AS bounces,

    -- Engagement
    COUNT(DISTINCT page_url) AS unique_pages_visited,

    -- Browser diversity
    COUNT(DISTINCT user_agent_id) AS user_agent_id_unique_count,

    -- Timestamps
    MIN(event_time) AS first_event_at,
    MAX(event_time) AS last_event_at,

    MAX(last_modified_at) as last_modified_at

FROM {{ ref('int_clicks_parsed') }} AS src
WHERE is_good_event = 1
    AND last_modified_at > {{ incremental_operand_safe('last_modified_at') }}
GROUP BY user_id, event_date
