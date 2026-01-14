-- Final fact model: atomic click events with contextual attributes.
-- No user-level aggregates and joins — they belong to fct_user_daily or other marts.
-- Purpose: single source of truth for click-level analysis.
-- by default - VIEW materialization used so it interativly updated any time without issues
SELECT
    icp.event_id,
    icp.event_time,
    icp.event_date,
    icp.user_id,
    icp.counter_id,

    icp.page_url,
    icp.page_title,
    icp.page_domain,
    icp.page_path,

    icp.referrer_url,
    icp.referrer_domain,
    icp.referrer_path,

    icp.utm_source,
    icp.utm_medium,
    icp.utm_campaign,
    icp.utm_source_from_url,
    icp.utm_medium_from_url,
    icp.utm_campaign_from_url,
    icp.referrer_utm_source,
    icp.referrer_utm_medium,

    icp.is_mobile,
    CASE WHEN icp.is_mobile = 1 THEN 'Mobile' ELSE 'Desktop' END AS device_type,

    icp.is_link_click,
    icp.is_download,
    icp.is_not_bounce,
    icp.is_good_event,
    icp.javascript_enabled,

    icp.traffic_source_id,
    CASE
        WHEN icp.utm_source IS NOT NULL
          OR icp.utm_source_from_url IS NOT NULL
          OR icp.referrer_utm_source IS NOT NULL
        THEN 'Paid'
        WHEN icp.referrer_domain IN ('google.com', 'yandex.ru', 'bing.com')
        THEN 'Organic'
        WHEN icp.referrer_url = '' OR icp.referrer_url IS NULL
        THEN 'Direct'
        ELSE 'Referral'
    END AS traffic_channel,

    icp.ecommerce_order_id,
    icp.product_price,
    icp.currency_code,

    icp.os_id,
    icp.user_agent_id,
    icp.region_id,
    icp.robot_score,

    icp.last_modified_at

FROM {{ ref('int_clicks_parsed') }} AS icp
WHERE icp.is_good_event = 1