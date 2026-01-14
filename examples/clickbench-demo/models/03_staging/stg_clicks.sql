SELECT
    last_modified_at,
    event_id,
    event_time,
    event_date,
    counter_id,
    user_id,

    traffic_source_id,
    utm_source,
    utm_medium,
    utm_campaign,
    referrer_url,

    {{ to_int8('is_mobile') }} AS is_mobile,
    os_id,
    user_agent_id,
    region_id,
    sex,
    age,
    income_level,

    page_url,
    page_title,

    {{ to_int8('is_link_click') }} AS is_link_click,
    {{ to_int8('is_download') }} AS is_download,
    {{ to_int8('is_not_bounce') }} AS is_not_bounce,
    {{ to_int8('is_good_event') }} AS is_good_event,

    ecommerce_order_id,
    product_price,
    currency_code,

    robot_score,
    {{ to_int8('javascript_enabled') }} AS javascript_enabled

FROM {{ ref('base_clicks') }}
WHERE last_modified_at > {{ incremental_operand_safe('last_modified_at') }}
