SELECT
    last_modified_at,
    event_id,
    user_id,
    counter_id,
    event_time,
    event_date,

    page_url,
    referrer_url,

    {{ url_get_host('page_url') }} AS page_domain,
    {{ url_get_path('page_url') }} AS page_path,
    {{ url_get_query('page_url') }} AS page_query,
    {{ url_get_fragment('page_url') }} AS page_fragment,

    {{ url_extract_param('page_url', 'utm_source') }} AS utm_source_from_url,
    {{ url_extract_param('page_url', 'utm_medium') }} AS utm_medium_from_url,
    {{ url_extract_param('page_url', 'utm_campaign') }} AS utm_campaign_from_url,
    {{ url_extract_param('page_url', 'utm_term') }} AS utm_term_from_url,
    {{ url_extract_param('page_url', 'utm_content') }} AS utm_content_from_url,

    {{ url_get_host('referrer_url') }} AS referrer_domain,
    {{ url_get_path('referrer_url') }} AS referrer_path,
    {{ url_get_query('referrer_url') }} AS referrer_query,
    {{ url_get_fragment('referrer_url') }} AS referrer_fragment,

    {{ url_extract_param('referrer_url', 'utm_source') }} AS referrer_utm_source,
    {{ url_extract_param('referrer_url', 'utm_medium') }} AS referrer_utm_medium,

    page_title,
    is_link_click,
    is_download,
    is_not_bounce,
    is_good_event,

    ecommerce_order_id,
    product_price,
    currency_code,

    is_mobile,
    os_id,
    user_agent_id,
    region_id,
    robot_score,
    javascript_enabled,

    traffic_source_id,
    utm_source,
    utm_medium,
    utm_campaign

FROM {{ ref('stg_clicks') }}
WHERE (page_url IS NOT NULL OR referrer_url IS NOT NULL)
        AND last_modified_at > {{ incremental_operand_safe('last_modified_at') }}
